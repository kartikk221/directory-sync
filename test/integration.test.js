import assert from 'node:assert/strict';
import FileSystem from 'node:fs/promises';
import Path from 'node:path';
import { setTimeout as wait } from 'node:timers/promises';
import test from 'node:test';
import { Mirror, Server } from '../index.js';
import {
    AUTH_HEADER,
    BASE_REVISION_HEADER,
    EPOCH_HEADER,
    HASH_HEADER,
    MODIFIED_HEADER,
    PROTOCOL_HEADER,
    PROTOCOL_PATH,
    PROTOCOL_VERSION,
} from '../src/constants.js';
import {
    available_port,
    capture_errors,
    read_if_exists,
    sha256,
    temporary_directory,
    wait_for,
    write_file,
} from './helpers.js';

const WATCHER = { awaitWriteFinish: false };

function endpoint(port, path, host = 'repository', uri) {
    const url = new URL(`http://127.0.0.1:${port}${path}`);
    url.searchParams.set('host', host);
    if (uri) url.searchParams.set('uri', uri);
    return url;
}

function headers(auth = 'secret', protocol = PROTOCOL_VERSION) {
    return {
        [AUTH_HEADER]: auth,
        [PROTOCOL_HEADER]: String(protocol),
    };
}

async function create_server(t, root, options = {}) {
    const port = options.port || await available_port();
    const server = new Server({ port, auth: 'secret', ...options });
    const errors = capture_errors(server);
    const logs = [];
    server.on('log', (code, message) => logs.push([code, message]));
    t.after(() => server.destroy());
    await server.ready();
    await server.host('repository', root, { watcher: WATCHER });
    return { errors, logs, port, server };
}

async function create_mirror(t, root, port) {
    const mirror = new Mirror('repository', {
        path: root,
        hostname: '127.0.0.1',
        port,
        auth: 'secret',
        retry: { every: 20, backoff: false },
        watcher: WATCHER,
    });
    const errors = capture_errors(mirror);
    const logs = [];
    mirror.on('log', (code, message) => logs.push([code, message]));
    t.after(() => mirror.destroy());
    await mirror.ready();
    return { errors, logs, mirror };
}

test('HyperExpress v7 routes enforce auth/protocol and stream exact file content', async (t) => {
    const root = await temporary_directory(t);
    const modified_at = Date.now() - 5_000;
    await write_file(root, '/nested/data.bin', Buffer.from([0, 1, 2, 3]), modified_at);
    const { errors, logs, port, server } = await create_server(t, root);

    assert.equal(typeof server.host, 'function');
    assert.equal(typeof server.destroy, 'function');
    assert.equal(server.options.auth, 'secret');
    assert.ok(server.hosts.repository);

    let response = await fetch(endpoint(port, `${PROTOCOL_PATH}/manifest`), {
        headers: headers('invalid'),
    });
    assert.equal(response.status, 403);
    assert.equal((await response.json()).code, 'UNAUTHORIZED');

    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/manifest`), {
        headers: headers('secret', PROTOCOL_VERSION - 1),
    });
    assert.equal(response.status, 426);
    assert.equal(response.headers.get(PROTOCOL_HEADER), String(PROTOCOL_VERSION));

    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/manifest`), { headers: headers() });
    assert.equal(response.status, 200);
    const manifest = await response.json();
    assert.equal(manifest.protocol, PROTOCOL_VERSION);
    assert.deepEqual(manifest.filters, { keep: {}, ignore: {} });
    assert.equal(typeof manifest.epoch, 'string');
    assert.equal(Number.isSafeInteger(manifest.server_time), true);
    assert.deepEqual(manifest.tombstones, []);
    assert.equal(manifest.versions['/nested/data.bin'], 0);
    assert.equal(manifest.manifest['/nested/data.bin'].sha256, sha256(Buffer.from([0, 1, 2, 3])));

    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/content`, 'repository', '/nested/data.bin'), {
        headers: headers(),
    });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '4');
    assert.equal(response.headers.get(MODIFIED_HEADER), String(modified_at));
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), Buffer.from([0, 1, 2, 3]));

    const uploaded = Buffer.from([4, 5, 6]);
    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/content`, 'repository', '/uploaded.bin'), {
        method: 'PUT',
        headers: {
            ...headers(),
            [MODIFIED_HEADER]: String(modified_at + 1_000),
            [HASH_HEADER]: sha256(uploaded),
            'content-length': String(uploaded.length),
        },
        body: uploaded,
    });
    assert.equal(response.status, 200);
    await response.json();
    await wait_for(() => logs.some(([code, message]) =>
        code === 'UPLOAD' &&
        message.includes("'repository' - /nested/data.bin - FILE - 4 BYTES - COMPLETE - ")
    ));
    assert.equal(logs.some(([code, message]) =>
        code === 'UPLOAD' &&
        message.includes("'repository' - /nested/data.bin - FILE - 4 BYTES - START")
    ), true);
    assert.equal(logs.some(([code, message]) =>
        code === 'DOWNLOAD' &&
        message.includes("'repository' - /uploaded.bin - FILE - 3 BYTES - START")
    ), true);
    assert.equal(logs.some(([code, message]) =>
        code === 'DOWNLOAD' &&
        message.includes("'repository' - /uploaded.bin - FILE - 3 BYTES - COMPLETE - ")
    ), true);
    assert.equal(logs.some(([code]) => code === 'MANIFEST'), true);
    assert.equal(logs.every(([code]) =>
        ['UPLOAD', 'DOWNLOAD', 'MANIFEST', 'WEBSOCKET', 'SIZE_LIMIT_REACHED', 'TOMBSTONE_EVICTED'].includes(code)
    ), true);
    assert.deepEqual(errors, []);
});

test('authority revisions serialize mutations and make retries idempotent', async (t) => {
    const root = await temporary_directory(t, 'directory-sync-revisions-');
    const base = Date.now() - 20_000;
    await write_file(root, '/shared.txt', 'initial', base);
    const { errors, port } = await create_server(t, root);

    let response = await fetch(endpoint(port, `${PROTOCOL_PATH}/manifest`), { headers: headers() });
    const manifest = await response.json();
    const revision_headers = (revision) => ({
        ...headers(),
        [EPOCH_HEADER]: manifest.epoch,
        [BASE_REVISION_HEADER]: String(revision),
    });
    const upload = (content, modified_at, revision) => fetch(
        endpoint(port, `${PROTOCOL_PATH}/content`, 'repository', '/shared.txt'),
        {
            method: 'PUT',
            headers: {
                ...revision_headers(revision),
                [MODIFIED_HEADER]: String(modified_at),
                [HASH_HEADER]: sha256(content),
                'content-length': String(Buffer.byteLength(content)),
            },
            body: content,
        }
    );

    response = await upload('first', base + 1_000, 0);
    assert.equal(response.status, 200);
    let payload = await response.json();
    assert.equal(payload.revision, 1);

    response = await upload('second', base + 2_000, 0);
    assert.equal(response.status, 200);
    payload = await response.json();
    assert.equal(payload.revision, 2);

    response = await upload('second', base + 2_000, 0);
    assert.equal(response.status, 200);
    payload = await response.json();
    assert.equal(payload.revision, 2);

    response = await upload('causal', base - 10_000, 2);
    assert.equal(response.status, 200);
    payload = await response.json();
    assert.equal(payload.revision, 3);
    assert.equal(await FileSystem.readFile(Path.join(root, 'shared.txt'), 'utf8'), 'causal');

    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/manifest`), { headers: headers() });
    const updated = await response.json();
    assert.equal(updated.epoch, manifest.epoch);
    assert.equal(updated.versions['/shared.txt'], 3);
    assert.deepEqual(errors, []);
});

test('a Server restart creates a new epoch and Mirrors converge new mutations', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-epoch-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-epoch-mirror-');
    const modified_at = Date.now() - 10_000;
    await write_file(server_root, '/epoch.txt', 'initial', modified_at);
    const original = await create_server(t, server_root);
    const mirror = await create_mirror(t, mirror_root, original.port);

    let response = await fetch(endpoint(original.port, `${PROTOCOL_PATH}/manifest`), {
        headers: { ...headers(), connection: 'close' },
    });
    const original_epoch = (await response.json()).epoch;
    await original.server.destroy();

    const replacement = new Server({ port: original.port, auth: 'secret' });
    const replacement_errors = capture_errors(replacement);
    t.after(() => replacement.destroy());
    await replacement.ready();
    await replacement.host('repository', server_root, { watcher: WATCHER });

    const replacement_manifest = await wait_for(async () => {
        try {
            response = await fetch(endpoint(original.port, `${PROTOCOL_PATH}/manifest`), {
                headers: { ...headers(), connection: 'close' },
            });
            if (!response.ok) return undefined;
            return response.json();
        } catch {
            return undefined;
        }
    });
    assert.notEqual(replacement_manifest.epoch, original_epoch);
    assert.equal(replacement_manifest.versions['/epoch.txt'], 0);

    await write_file(mirror_root, '/epoch.txt', 'after-restart', modified_at + 5_000);
    await wait_for(async () =>
        (await read_if_exists(Path.join(server_root, 'epoch.txt'))) === 'after-restart',
    { diagnostic: async () => ({
        server: replacement.hosts.repository.map.get_entry('/epoch.txt'),
        mirror: mirror.mirror.map.get_entry('/epoch.txt'),
        mirror_errors: mirror.errors.map((error) => error.message),
        replacement_errors: replacement_errors.map((error) => error.message),
    }) });
    assert.equal(mirror.logs.some(([code, message]) =>
        code === 'WEBSOCKET' && message.startsWith('DISCONNECTED - ')
    ), true);
    assert.deepEqual(mirror.errors, []);
    assert.deepEqual(replacement_errors, []);
});

test('server rejects malformed, oversized, stale, and filtered mutations', async (t) => {
    const root = await temporary_directory(t);
    await write_file(root, '/current.txt', 'central', Date.now());
    const port = await available_port();
    const server = new Server({
        port,
        auth: 'secret',
        limits: { max_body_length: 128, fast_buffers: true },
    });
    const errors = capture_errors(server);
    t.after(() => server.destroy());
    await server.ready();
    await server.host('repository', root, {
        watcher: WATCHER,
        filters: { keep: { extensions: ['.txt'] } },
    });

    let response = await fetch(endpoint(port, `${PROTOCOL_PATH}/content`, 'repository', '/large.txt'), {
        method: 'PUT',
        headers: {
            ...headers(),
            [MODIFIED_HEADER]: String(Date.now() + 1_000),
            [HASH_HEADER]: sha256('x'.repeat(129)),
            'content-length': '129',
        },
        body: 'x'.repeat(129),
    });
    assert.equal(response.status, 413);

    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/content`, 'repository', '/bad.txt'), {
        method: 'PUT',
        headers: {
            ...headers(),
            [MODIFIED_HEADER]: String(Date.now() + 1_000),
            [HASH_HEADER]: '0'.repeat(64),
            'content-length': '3',
        },
        body: 'bad',
    });
    assert.equal(response.status, 422);
    assert.equal((await response.json()).code, 'CHECKSUM');

    const current = server.hosts.repository.map.get_entry('/current.txt');
    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/entry`, 'repository', '/current.txt'), {
        method: 'DELETE',
        headers: { ...headers(), 'content-type': 'application/json' },
        body: JSON.stringify({
            type: 'tombstone',
            target: 'file',
            deleted_at: current.modified_at - 1,
        }),
    });
    assert.equal(response.status, 409);
    assert.equal((await response.json()).entry.type, 'file');

    response = await fetch(endpoint(port, `${PROTOCOL_PATH}/entry`, 'repository', '/blocked.bin'), {
        method: 'DELETE',
        headers: { ...headers(), 'content-type': 'application/json' },
        body: JSON.stringify({ type: 'tombstone', target: 'file', deleted_at: Date.now() }),
    });
    assert.equal(response.status, 422);
    assert.equal((await response.json()).code, 'FILTERED');
    assert.deepEqual(errors, []);
});

test('Mirror drops authority-filtered paths before transfers or logs', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-filter-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-filter-mirror-');
    await write_file(server_root, '/download.txt', 'download');
    await write_file(mirror_root, '/allowed.txt', 'allowed');
    await write_file(mirror_root, '/blocked/rejected.txt', 'blocked');
    const port = await available_port();
    const server = new Server({ port, auth: 'secret' });
    const server_errors = capture_errors(server);
    t.after(() => server.destroy());
    await server.ready();
    await server.host('repository', server_root, {
        watcher: WATCHER,
        filters: { ignore: { directories: ['blocked'] } },
    });

    const { errors, logs, mirror } = await create_mirror(t, mirror_root, port);
    assert.equal(await read_if_exists(Path.join(server_root, 'allowed.txt')), 'allowed');
    assert.equal(await read_if_exists(Path.join(mirror_root, 'download.txt')), 'download');
    assert.equal(await read_if_exists(Path.join(server_root, 'blocked/rejected.txt')), undefined);
    assert.equal(logs.some(([code, message]) =>
        code === 'UPLOAD' && message === '/allowed.txt - FILE - START'
    ), true);
    assert.equal(logs.some(([code, message]) =>
        code === 'UPLOAD' && message.startsWith('/allowed.txt - FILE - COMPLETE - ')
    ), true);
    assert.equal(logs.some(([code, message]) =>
        code === 'DOWNLOAD' && message === '/download.txt - FILE - START'
    ), true);
    assert.equal(logs.some(([code, message]) =>
        code === 'DOWNLOAD' && message.startsWith('/download.txt - FILE - COMPLETE - ')
    ), true);
    assert.equal(logs.some(([code, message]) =>
        code === 'FILTERED' || message.startsWith('/blocked')
    ), false);
    await write_file(mirror_root, '/nested/blocked/live.txt', 'blocked');
    await wait(200);
    assert.equal(mirror.map.get_entry('/nested/blocked/live.txt'), undefined);
    assert.equal(await read_if_exists(Path.join(server_root, 'nested/blocked/live.txt')), undefined);
    assert.equal(logs.some(([, message]) => message.includes('/nested/blocked/live.txt')), false);
    assert.equal(logs.every(([code]) =>
        ['UPLOAD', 'DOWNLOAD', 'MANIFEST', 'WEBSOCKET', 'SIZE_LIMIT_REACHED', 'TOMBSTONE_EVICTED'].includes(code)
    ), true);
    assert.deepEqual(errors, []);
    assert.deepEqual(server_errors, []);
});

test('server authority converges two mirrors with newest-mtime conflict resolution and tombstones', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-server-');
    const first_root = await temporary_directory(t, 'directory-sync-first-');
    const second_root = await temporary_directory(t, 'directory-sync-second-');
    const base = Date.now() - 20_000;
    await write_file(server_root, '/shared.txt', 'initial', base);
    const server_record = await create_server(t, server_root);
    let first_record = await create_mirror(t, first_root, server_record.port);
    const second_record = await create_mirror(t, second_root, server_record.port);

    await wait_for(async () =>
        (await read_if_exists(Path.join(first_root, 'shared.txt'))) === 'initial' &&
        (await read_if_exists(Path.join(second_root, 'shared.txt'))) === 'initial'
    );

    const mirror_time = base + 5_000;
    await write_file(first_root, '/shared.txt', 'from-first', mirror_time);
    await wait_for(async () => (
        (await read_if_exists(Path.join(server_root, 'shared.txt'))) === 'from-first' &&
        (await read_if_exists(Path.join(second_root, 'shared.txt'))) === 'from-first'
    ), { diagnostic: async () => ({
        server_content: await read_if_exists(Path.join(server_root, 'shared.txt')),
        first_content: await read_if_exists(Path.join(first_root, 'shared.txt')),
        second_content: await read_if_exists(Path.join(second_root, 'shared.txt')),
        server: server_record.server.hosts.repository.map.canonical_entry('/shared.txt'),
        first: first_record.mirror.map.canonical_entry('/shared.txt'),
        second: second_record.mirror.map.canonical_entry('/shared.txt'),
        first_watched: first_record.mirror.map.watcher.getWatched(),
        server_errors: server_record.errors.map((error) => error.message),
        first_errors: first_record.errors.map((error) => error.message),
        second_errors: second_record.errors.map((error) => error.message),
    }) });
    await wait_for(() =>
        server_record.server.hosts.repository.map.get_entry('/shared.txt')?.sha256 === sha256('from-first') &&
        second_record.mirror.map.get_entry('/shared.txt')?.sha256 === sha256('from-first')
    );

    const authority_time = mirror_time + 5_000;
    await write_file(server_root, '/shared.txt', 'from-authority', authority_time);
    await wait_for(() => (
        server_record.server.hosts.repository.map.get_entry('/shared.txt')?.sha256 === sha256('from-authority')
    ), { diagnostic: async () => ({
        content: await read_if_exists(Path.join(server_root, 'shared.txt')),
        entry: server_record.server.hosts.repository.map.get_entry('/shared.txt'),
        watched: server_record.server.hosts.repository.map.watcher.getWatched(),
        errors: server_record.errors.map((error) => error.message),
    }) });
    await wait_for(async () => (
        (await read_if_exists(Path.join(first_root, 'shared.txt'))) === 'from-authority' &&
        (await read_if_exists(Path.join(second_root, 'shared.txt'))) === 'from-authority'
    ), { diagnostic: async () => ({
        server: server_record.server.hosts.repository.map.get_entry('/shared.txt'),
        first: first_record.mirror.map.get_entry('/shared.txt'),
        second: second_record.mirror.map.get_entry('/shared.txt'),
        server_errors: server_record.errors.map((error) => error.message),
        first_errors: first_record.errors.map((error) => error.message),
        second_errors: second_record.errors.map((error) => error.message),
    }) });

    await first_record.mirror.destroy();
    await write_file(first_root, '/shared.txt', 'tie-loses', authority_time);
    first_record = await create_mirror(t, first_root, server_record.port);
    await wait_for(async () =>
        (await read_if_exists(Path.join(first_root, 'shared.txt'))) === 'from-authority'
    );
    assert.equal(await FileSystem.readFile(Path.join(server_root, 'shared.txt'), 'utf8'), 'from-authority');

    await first_record.mirror.destroy();
    await FileSystem.rm(Path.join(first_root, 'shared.txt'));
    first_record = await create_mirror(t, first_root, server_record.port);
    await wait_for(async () =>
        (await read_if_exists(Path.join(first_root, 'shared.txt'))) === 'from-authority'
    );

    await FileSystem.rm(Path.join(first_root, 'shared.txt'));
    await wait_for(async () => (
        (await read_if_exists(Path.join(server_root, 'shared.txt'))) === undefined &&
        (await read_if_exists(Path.join(second_root, 'shared.txt'))) === undefined
    ), { diagnostic: async () => ({
        server_content: await read_if_exists(Path.join(server_root, 'shared.txt')),
        first: first_record.mirror.map.canonical_entry('/shared.txt'),
        server: server_record.server.hosts.repository.map.canonical_entry('/shared.txt'),
        tombstone: server_record.server.hosts.repository.map.get_tombstone('/shared.txt'),
        server_errors: server_record.errors.map((error) => error.message),
        first_errors: first_record.errors.map((error) => error.message),
        second_errors: second_record.errors.map((error) => error.message),
    }) });
    assert.equal(server_record.server.hosts.repository.map.manifest['/shared.txt'], undefined);

    const deletion = server_record.server.hosts.repository.map.get_tombstone('/shared.txt');
    assert.equal(deletion.type, 'tombstone');
    await wait(150);
    await write_file(server_root, '/shared.txt', 'resurrected', deletion.deleted_at + 1);
    await wait_for(() =>
        server_record.server.hosts.repository.map.canonical_entry('/shared.txt')?.type === 'file'
    );
    await wait_for(async () => (
        (await read_if_exists(Path.join(first_root, 'shared.txt'))) === 'resurrected' &&
        (await read_if_exists(Path.join(second_root, 'shared.txt'))) === 'resurrected'
    ), { diagnostic: async () => ({
        server_content: await read_if_exists(Path.join(server_root, 'shared.txt')),
        first_content: await read_if_exists(Path.join(first_root, 'shared.txt')),
        second_content: await read_if_exists(Path.join(second_root, 'shared.txt')),
        server: server_record.server.hosts.repository.map.canonical_entry('/shared.txt'),
        first: first_record.mirror.map.canonical_entry('/shared.txt'),
        second: second_record.mirror.map.canonical_entry('/shared.txt'),
        server_errors: server_record.errors.map((error) => error.message),
        first_errors: first_record.errors.map((error) => error.message),
        second_errors: second_record.errors.map((error) => error.message),
    }) });

    let response = await fetch(endpoint(server_record.port, `${PROTOCOL_PATH}/manifest`), { headers: headers() });
    const stable_revision = (await response.json()).versions['/shared.txt'];
    await wait(500);
    response = await fetch(endpoint(server_record.port, `${PROTOCOL_PATH}/manifest`), { headers: headers() });
    assert.equal((await response.json()).versions['/shared.txt'], stable_revision);

    assert.deepEqual(server_record.errors, []);
    assert.deepEqual(first_record.errors, []);
    assert.deepEqual(second_record.errors, []);
});

test('zero-byte files and directory/file transitions converge across mirrors', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-mirror-');
    const server_record = await create_server(t, server_root);
    const mirror_record = await create_mirror(t, mirror_root, server_record.port);

    await write_file(mirror_root, '/empty.txt', Buffer.alloc(0), Date.now() - 1_000);
    await wait_for(async () => {
        try {
            return (await FileSystem.lstat(Path.join(server_root, 'empty.txt'))).size === 0;
        } catch {
            return false;
        }
    }, { diagnostic: async () => ({
        server: server_record.server.hosts.repository.map.get_entry('/empty.txt'),
        mirror: mirror_record.mirror.map.get_entry('/empty.txt'),
        server_errors: server_record.errors.map((error) => error.message),
        mirror_errors: mirror_record.errors.map((error) => error.message),
    }) });

    await FileSystem.rm(Path.join(mirror_root, 'empty.txt'));
    await FileSystem.mkdir(Path.join(mirror_root, 'empty.txt'));
    await wait_for(() => mirror_record.mirror.map.get_entry('/empty.txt')?.type === 'directory');
    await wait_for(async () => {
        try {
            return (await FileSystem.lstat(Path.join(server_root, 'empty.txt'))).isDirectory();
        } catch {
            return false;
        }
    });
    await wait_for(() =>
        server_record.server.hosts.repository.map.get_entry('/empty.txt')?.type === 'directory'
    );

    await FileSystem.rm(Path.join(server_root, 'empty.txt'), { recursive: true });
    await wait_for(() =>
        server_record.server.hosts.repository.map.canonical_entry('/empty.txt')?.type === 'tombstone',
    { diagnostic: async () => ({
        server: server_record.server.hosts.repository.map.get_entry('/empty.txt'),
        tombstone: server_record.server.hosts.repository.map.get_tombstone('/empty.txt'),
        errors: server_record.errors.map((error) => error.message),
    }) });
    await wait(150);
    await write_file(server_root, '/empty.txt', 'file-again', Date.now() + 3_000);
    await wait_for(() =>
        server_record.server.hosts.repository.map.canonical_entry('/empty.txt')?.type === 'file'
    );
    await wait_for(async () =>
        (await read_if_exists(Path.join(mirror_root, 'empty.txt'))) === 'file-again',
    { diagnostic: async () => ({
        server: server_record.server.hosts.repository.map.canonical_entry('/empty.txt'),
        mirror: mirror_record.mirror.map.canonical_entry('/empty.txt'),
        server_errors: server_record.errors.map((error) => error.message),
        mirror_errors: mirror_record.errors.map((error) => error.message),
    }) });
    assert.deepEqual(server_record.errors, []);
    assert.deepEqual(mirror_record.errors, []);
});
