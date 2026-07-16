import assert from 'node:assert/strict';
import FileSystem from 'node:fs/promises';
import Path from 'node:path';
import { setTimeout as wait } from 'node:timers/promises';
import test from 'node:test';
import { Mirror, Server } from '../index.js';
import {
    AUTH_HEADER,
    HASH_HEADER,
    MODIFIED_HEADER,
    PROTOCOL_HEADER,
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
    t.after(() => server.destroy());
    await server.ready();
    await server.host('repository', root, { watcher: WATCHER });
    return { errors, port, server };
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
    t.after(() => mirror.destroy());
    await mirror.ready();
    return { errors, mirror };
}

test('HyperExpress v7 routes enforce auth/protocol and stream exact file content', async (t) => {
    const root = await temporary_directory(t);
    const modified_at = Date.now() - 5_000;
    await write_file(root, '/nested/data.bin', Buffer.from([0, 1, 2, 3]), modified_at);
    const { errors, port, server } = await create_server(t, root);

    assert.equal(typeof server.host, 'function');
    assert.equal(typeof server.destroy, 'function');
    assert.equal(server.options.auth, 'secret');
    assert.ok(server.hosts.repository);

    let response = await fetch(endpoint(port, '/v3/manifest'), {
        headers: headers('invalid'),
    });
    assert.equal(response.status, 403);
    assert.equal((await response.json()).code, 'UNAUTHORIZED');

    response = await fetch(endpoint(port, '/v3/manifest'), {
        headers: headers('secret', PROTOCOL_VERSION - 1),
    });
    assert.equal(response.status, 426);
    assert.equal(response.headers.get(PROTOCOL_HEADER), String(PROTOCOL_VERSION));

    response = await fetch(endpoint(port, '/v3/manifest'), { headers: headers() });
    assert.equal(response.status, 200);
    const manifest = await response.json();
    assert.equal(manifest.protocol, PROTOCOL_VERSION);
    assert.equal(manifest.manifest['/nested/data.bin'].sha256, sha256(Buffer.from([0, 1, 2, 3])));

    response = await fetch(endpoint(port, '/v3/content', 'repository', '/nested/data.bin'), {
        headers: headers(),
    });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('content-length'), '4');
    assert.equal(response.headers.get(MODIFIED_HEADER), String(modified_at));
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), Buffer.from([0, 1, 2, 3]));
    assert.deepEqual(errors, []);
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

    let response = await fetch(endpoint(port, '/v3/content', 'repository', '/large.txt'), {
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

    response = await fetch(endpoint(port, '/v3/content', 'repository', '/bad.txt'), {
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
    response = await fetch(endpoint(port, '/v3/entry', 'repository', '/current.txt'), {
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

    response = await fetch(endpoint(port, '/v3/entry', 'repository', '/blocked.bin'), {
        method: 'DELETE',
        headers: { ...headers(), 'content-type': 'application/json' },
        body: JSON.stringify({ type: 'tombstone', target: 'file', deleted_at: Date.now() }),
    });
    assert.equal(response.status, 422);
    assert.equal((await response.json()).code, 'FILTERED');
    assert.deepEqual(errors, []);
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
    await wait_for(async () =>
        (await read_if_exists(Path.join(server_root, 'shared.txt'))) === 'from-first' &&
        (await read_if_exists(Path.join(second_root, 'shared.txt'))) === 'from-first'
    );
    await wait_for(() =>
        server_record.server.hosts.repository.map.get_entry('/shared.txt')?.sha256 === sha256('from-first') &&
        second_record.mirror.map.get_entry('/shared.txt')?.sha256 === sha256('from-first')
    );

    const authority_time = mirror_time + 5_000;
    await write_file(server_root, '/shared.txt', 'from-authority', authority_time);
    await wait_for(() =>
        server_record.server.hosts.repository.map.get_entry('/shared.txt')?.sha256 === sha256('from-authority')
    );
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
        (await read_if_exists(Path.join(server_root, 'shared.txt'))) === undefined &&
        (await read_if_exists(Path.join(second_root, 'shared.txt'))) === undefined
    );
    assert.equal(server_record.server.hosts.repository.map.manifest['/shared.txt'].type, 'tombstone');

    const deletion = server_record.server.hosts.repository.map.manifest['/shared.txt'];
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
