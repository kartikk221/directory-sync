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

async function create_mirror(t, root, port, options = {}) {
    const mirror = new Mirror('repository', {
        path: root,
        hostname: '127.0.0.1',
        port,
        auth: 'secret',
        retry: { every: 20, backoff: false },
        watcher: WATCHER,
        ...options,
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
        ['UPLOAD', 'DOWNLOAD', 'MANIFEST', 'WEBSOCKET'].includes(code)
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

test('live Server file bursts download concurrently through the Mirror transfer queue', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-live-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-live-mirror-');
    const server_record = await create_server(t, server_root);
    const mirror_record = await create_mirror(t, mirror_root, server_record.port);
    const targets = new Set(['/parallel-a.txt', '/parallel-b.txt']);
    const original_fetch = globalThis.fetch;
    const requests = [];
    let release_first;
    let overlapped = false;
    const first_gate = new Promise((resolve) => {
        release_first = resolve;
    });
    globalThis.fetch = async (input, options) => {
        const url = new URL(input);
        const uri = url.searchParams.get('uri');
        if (
            (options?.method || 'GET') === 'GET' &&
            url.pathname === `${PROTOCOL_PATH}/content` &&
            targets.has(uri)
        ) {
            requests.push(uri);
            if (requests.length === 1) {
                await Promise.race([
                    first_gate,
                    wait(750, undefined, { ref: false }),
                ]);
            } else {
                overlapped = true;
                release_first();
            }
        }
        return original_fetch(input, options);
    };
    t.after(() => {
        release_first();
        globalThis.fetch = original_fetch;
    });

    const modified_at = Date.now();
    await Promise.all([
        write_file(server_root, '/parallel-a.txt', 'alpha', modified_at),
        write_file(server_root, '/parallel-b.txt', 'bravo', modified_at + 1),
    ]);
    await wait_for(async () =>
        (await read_if_exists(Path.join(mirror_root, 'parallel-a.txt'))) === 'alpha' &&
        (await read_if_exists(Path.join(mirror_root, 'parallel-b.txt'))) === 'bravo'
    );

    assert.equal(overlapped, true);
    assert.deepEqual(new Set(requests), targets);
    assert.deepEqual(mirror_record.errors, []);
    assert.deepEqual(server_record.errors, []);
});

test('live mutations coalesce to the newest pending revision for each URI', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-coalesce-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-coalesce-mirror-');
    const server_record = await create_server(t, server_root);
    const mirror_record = await create_mirror(t, mirror_root, server_record.port, {
        queue: { max_concurrent: 2, max_pending: 2 },
    });
    const response = await fetch(endpoint(server_record.port, `${PROTOCOL_PATH}/manifest`), {
        headers: headers(),
    });
    const { epoch } = await response.json();
    const empty = {
        type: 'file',
        modified_at: Date.now(),
        size: 0,
        sha256: sha256(''),
    };
    const applied = [];
    let release_block;
    let signal_block;
    const block_gate = new Promise((resolve) => {
        release_block = resolve;
    });
    const block_started = new Promise((resolve) => {
        signal_block = resolve;
    });
    mirror_record.mirror._apply_remote = async (uri, entry, revision) => {
        if (uri === '/block.txt') {
            signal_block();
            await block_gate;
        } else applied.push([uri, entry.modified_at, revision]);
    };
    t.after(() => release_block());

    mirror_record.mirror._receive_mutation({
        actor: 'synthetic',
        command: 'MUTATION',
        entry: empty,
        epoch,
        host: 'repository',
        protocol: PROTOCOL_VERSION,
        revision: 1,
        uri: '/block.txt',
    });
    await block_started;
    for (let revision = 2; revision <= 101; revision++) {
        mirror_record.mirror._receive_mutation({
            actor: 'synthetic',
            command: 'MUTATION',
            entry: { ...empty, modified_at: empty.modified_at + revision },
            epoch,
            host: 'repository',
            protocol: PROTOCOL_VERSION,
            revision,
            uri: '/coalesced.txt',
        });
    }
    release_block();
    await wait_for(() => applied.length === 1);
    await wait(25);

    assert.deepEqual(applied, [[
        '/coalesced.txt',
        empty.modified_at + 101,
        101,
    ]]);
    assert.equal(mirror_record.logs.filter(([code]) => code === 'MANIFEST').length, 1);
    assert.deepEqual(mirror_record.errors, []);
    assert.deepEqual(server_record.errors, []);
});

test('live inbox overflow falls back to bounded authoritative reconciliation', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-overflow-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-overflow-mirror-');
    const server_record = await create_server(t, server_root);
    const mirror_record = await create_mirror(t, mirror_root, server_record.port, {
        queue: { max_concurrent: 2, max_queued: 0, max_pending: 2 },
    });
    const original_fetch = globalThis.fetch;
    const targets = Array.from({ length: 8 }, (_, index) =>
        `/overflow-${String(index).padStart(2, '0')}.txt`
    );
    let release_download;
    let signal_download;
    let received = 0;
    const download_gate = new Promise((resolve) => {
        release_download = resolve;
    });
    const download_started = new Promise((resolve) => {
        signal_download = resolve;
    });
    globalThis.fetch = async (input, options) => {
        const url = new URL(input);
        const response = await original_fetch(input, options);
        if (
            (options?.method || 'GET') === 'GET' &&
            url.pathname === `${PROTOCOL_PATH}/content` &&
            url.searchParams.get('uri') === '/block-overflow.txt'
        ) {
            signal_download();
            await download_gate;
        }
        return response;
    };
    const receive_mutation = mirror_record.mirror._receive_mutation.bind(mirror_record.mirror);
    mirror_record.mirror._receive_mutation = (message) => {
        if (targets.includes(message.uri)) received++;
        return receive_mutation(message);
    };
    t.after(() => {
        release_download();
        globalThis.fetch = original_fetch;
    });

    await write_file(server_root, '/block-overflow.txt', 'block');
    await download_started;
    await Promise.all(targets.map((uri, index) =>
        write_file(server_root, uri, `content-${index}`)
    ));
    await wait_for(() => received === targets.length);
    release_download();
    await wait_for(async () => {
        for (let index = 0; index < targets.length; index++)
            if (
                (await read_if_exists(Path.join(mirror_root, targets[index]))) !==
                `content-${index}`
            ) return false;
        return true;
    });

    assert.ok(mirror_record.logs.filter(([code]) => code === 'MANIFEST').length >= 2);
    assert.deepEqual(mirror_record.errors, []);
    assert.deepEqual(server_record.errors, []);
});

test('live Mirror upload bursts use the same bounded reconciliation fallback', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-upload-overflow-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-upload-overflow-mirror-');
    const server_record = await create_server(t, server_root);
    const mirror_record = await create_mirror(t, mirror_root, server_record.port, {
        queue: { max_concurrent: 2, max_queued: 0, max_pending: 2 },
    });
    const original_fetch = globalThis.fetch;
    const targets = Array.from({ length: 8 }, (_, index) =>
        `/upload-overflow-${String(index).padStart(2, '0')}.txt`
    );
    let release_upload;
    let signal_upload;
    let received = 0;
    const upload_gate = new Promise((resolve) => {
        release_upload = resolve;
    });
    const upload_started = new Promise((resolve) => {
        signal_upload = resolve;
    });
    globalThis.fetch = async (input, options) => {
        const url = new URL(input);
        if (
            options?.method === 'PUT' &&
            url.pathname === `${PROTOCOL_PATH}/content` &&
            url.searchParams.get('uri') === '/block-upload.txt'
        ) {
            signal_upload();
            await upload_gate;
        }
        return original_fetch(input, options);
    };
    const handle_local_mutation = mirror_record.mirror._handle_local_mutation
        .bind(mirror_record.mirror);
    mirror_record.mirror._handle_local_mutation = (uri, entry) => {
        if (targets.includes(uri)) received++;
        return handle_local_mutation(uri, entry);
    };
    t.after(() => {
        release_upload();
        globalThis.fetch = original_fetch;
    });

    await write_file(mirror_root, '/block-upload.txt', 'block');
    await upload_started;
    await Promise.all(targets.map((uri, index) =>
        write_file(mirror_root, uri, `upload-${index}`)
    ));
    await wait_for(() => received === targets.length);
    release_upload();
    await wait_for(async () => {
        for (let index = 0; index < targets.length; index++)
            if (
                (await read_if_exists(Path.join(server_root, targets[index]))) !==
                `upload-${index}`
            ) return false;
        return true;
    });

    assert.ok(mirror_record.logs.filter(([code]) => code === 'MANIFEST').length >= 2);
    assert.deepEqual(mirror_record.errors, []);
    assert.deepEqual(server_record.errors, []);
});

test('complete reconciliation does not require queued transfer promises', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-bounded-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-bounded-mirror-');
    const targets = Array.from({ length: 12 }, (_, index) =>
        `/bounded-${String(index).padStart(2, '0')}.txt`
    );
    await Promise.all(targets.map((uri, index) =>
        write_file(server_root, uri, `bounded-${index}`)
    ));
    const server_record = await create_server(t, server_root);
    const mirror_record = await create_mirror(t, mirror_root, server_record.port, {
        queue: { max_concurrent: 2, max_queued: 0, max_pending: 2 },
    });

    for (let index = 0; index < targets.length; index++)
        assert.equal(
            await read_if_exists(Path.join(mirror_root, targets[index])),
            `bounded-${index}`
        );
    assert.deepEqual(mirror_record.errors, []);
    assert.deepEqual(server_record.errors, []);
    await mirror_record.mirror.destroy();
    await server_record.server.destroy();
});

test('live directory mutations remain barriers around descendant file transfers', async (t) => {
    const server_root = await temporary_directory(t, 'directory-sync-barrier-server-');
    const mirror_root = await temporary_directory(t, 'directory-sync-barrier-mirror-');
    const server_record = await create_server(t, server_root);
    const mirror_record = await create_mirror(t, mirror_root, server_record.port);
    const original_fetch = globalThis.fetch;
    let release_download;
    let signal_download;
    let signal_tombstone;
    const download_gate = new Promise((resolve) => {
        release_download = resolve;
    });
    const download_started = new Promise((resolve) => {
        signal_download = resolve;
    });
    const tombstone_received = new Promise((resolve) => {
        signal_tombstone = resolve;
    });
    globalThis.fetch = async (input, options) => {
        const url = new URL(input);
        const response = await original_fetch(input, options);
        if (
            (options?.method || 'GET') === 'GET' &&
            url.pathname === `${PROTOCOL_PATH}/content` &&
            url.searchParams.get('uri') === '/tree/payload.txt'
        ) {
            signal_download();
            await download_gate;
        }
        return response;
    };
    const receive_mutation = mirror_record.mirror._receive_mutation.bind(mirror_record.mirror);
    mirror_record.mirror._receive_mutation = (message) => {
        if (
            message.uri === '/tree' &&
            message.entry?.type === 'tombstone' &&
            message.entry.target === 'directory'
        ) signal_tombstone();
        return receive_mutation(message);
    };
    t.after(() => {
        release_download();
        globalThis.fetch = original_fetch;
    });

    await write_file(server_root, '/tree/payload.txt', 'payload');
    await download_started;
    await FileSystem.rm(Path.join(server_root, 'tree'), { recursive: true, force: true });
    await tombstone_received;
    await wait(50);
    assert.equal((await FileSystem.stat(Path.join(mirror_root, 'tree'))).isDirectory(), true);

    release_download();
    await wait_for(async () => {
        try {
            await FileSystem.stat(Path.join(mirror_root, 'tree'));
            return false;
        } catch (error) {
            if (error.code === 'ENOENT') return true;
            throw error;
        }
    });
    assert.deepEqual(mirror_record.errors, []);
    assert.deepEqual(server_record.errors, []);
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

    const original_fetch = globalThis.fetch;
    const requested_uris = [];
    globalThis.fetch = (input, options) => {
        requested_uris.push(new URL(input).searchParams.get('uri'));
        return original_fetch(input, options);
    };
    t.after(() => { globalThis.fetch = original_fetch; });
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
    assert.equal(requested_uris.some((uri) => uri?.includes('/blocked')), false);
    assert.equal(logs.every(([code]) =>
        ['UPLOAD', 'DOWNLOAD', 'MANIFEST', 'WEBSOCKET'].includes(code)
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
