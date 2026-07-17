import Crypto from 'node:crypto';
import FileSystem from 'node:fs/promises';
import Net from 'node:net';
import OS from 'node:os';
import Path from 'node:path';
import { setTimeout as wait } from 'node:timers/promises';
import { Mirror, Server } from '../index.js';
import { generate_sha256_hash } from '../src/utils/operators.js';

const files = Number.parseInt(process.env.BENCHMARK_FILES || '100', 10);
const bytes = Number.parseInt(process.env.BENCHMARK_FILE_SIZE || String(1024 * 1024), 10);
const concurrency = Number.parseInt(process.env.BENCHMARK_CONCURRENCY || '100', 10);
if (!Number.isSafeInteger(files) || files < 2) throw new TypeError('BENCHMARK_FILES must be at least two.');
if (!Number.isSafeInteger(bytes) || bytes < 1) throw new TypeError('BENCHMARK_FILE_SIZE must be positive.');
if (!Number.isSafeInteger(concurrency) || concurrency < 1)
    throw new TypeError('BENCHMARK_CONCURRENCY must be positive.');

function available_port() {
    return new Promise((resolve, reject) => {
        const server = Net.createServer();
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
            const port = server.address().port;
            server.close((error) => error ? reject(error) : resolve(port));
        });
    });
}

function resolve_uri(root, uri) {
    return Path.join(root, ...uri.split('/').filter(Boolean));
}

async function in_batches(items, size, handler) {
    for (let index = 0; index < items.length; index += size) {
        const batch = [];
        const end = Math.min(items.length, index + size);
        for (let cursor = index; cursor < end; cursor++) batch.push(handler(items[cursor]));
        await Promise.all(batch);
    }
}

async function write_random(root, uri) {
    const content = Crypto.randomBytes(bytes);
    const path = resolve_uri(root, uri);
    await FileSystem.mkdir(Path.dirname(path), { recursive: true });
    await FileSystem.writeFile(path, content);
    return Crypto.createHash('sha256').update(content).digest('hex');
}

async function wait_for(predicate, timeout = 60_000) {
    const started = Date.now();
    while (Date.now() - started < timeout) {
        if (await predicate()) return;
        await wait(25);
    }
    throw new Error(`Stress convergence was not reached within ${timeout}ms.`);
}

function maps_match(server, mirror, expected, deleted) {
    for (const [uri, sha256] of expected) {
        const authority = server.hosts.benchmark.map.get_entry(uri);
        const replica = mirror.map.get_entry(uri);
        if (
            authority?.type !== 'file' || replica?.type !== 'file' ||
            authority.sha256 !== sha256 || replica.sha256 !== sha256
        ) return false;
    }
    for (const uri of deleted)
        if (server.hosts.benchmark.map.get_entry(uri) || mirror.map.get_entry(uri)) return false;
    return true;
}

async function verify_integrity(root, expected, deleted) {
    await in_batches([...expected], 10, async ([uri, sha256]) => {
        const actual = await generate_sha256_hash(resolve_uri(root, uri));
        if (actual !== sha256) throw new Error(`Integrity mismatch for ${uri} in ${root}.`);
    });
    for (const uri of deleted) {
        try {
            await FileSystem.lstat(resolve_uri(root, uri));
            throw new Error(`Deleted path still exists: ${uri} in ${root}.`);
        } catch (error) {
            if (error.code !== 'ENOENT') throw error;
        }
    }
}

const source = await FileSystem.mkdtemp(Path.join(OS.tmpdir(), 'directory-sync-benchmark-source-'));
const target = await FileSystem.mkdtemp(Path.join(OS.tmpdir(), 'directory-sync-benchmark-target-'));
const expected = new Map();
const deleted = new Set();
const errors = [];
let mirror;
let server;

try {
    const initial = [];
    const server_files = Math.ceil(files / 2);
    for (let index = 0; index < files; index++) {
        const server_side = index < server_files;
        initial.push({
            root: server_side ? source : target,
            uri: `/${server_side ? 'server' : 'mirror'}-${String(index).padStart(4, '0')}.bin`,
        });
    }
    await in_batches(initial, 10, async ({ root, uri }) => expected.set(uri, await write_random(root, uri)));

    const port = await available_port();
    server = new Server({ port, auth: 'benchmark' });
    server.on('error', (error) => errors.push(error));
    await server.ready();
    await server.host('benchmark', source, { watcher: { awaitWriteFinish: false } });

    const started = performance.now();
    mirror = new Mirror('benchmark', {
        path: target,
        hostname: '127.0.0.1',
        port,
        auth: 'benchmark',
        queue: { max_concurrent: concurrency },
        watcher: { awaitWriteFinish: false },
    });
    mirror.on('error', (error) => errors.push(error));
    await mirror.ready();
    const initial_duration = performance.now() - started;
    if (!maps_match(server, mirror, expected, deleted))
        throw new Error('Initial bidirectional manifests did not converge.');
    await Promise.all([
        verify_integrity(source, expected, deleted),
        verify_integrity(target, expected, deleted),
    ]);

    const server_deletion = '/server-0000.bin';
    const mirror_deletion = `/mirror-${String(server_files).padStart(4, '0')}.bin`;
    deleted.add(server_deletion);
    deleted.add(mirror_deletion);
    expected.delete(server_deletion);
    expected.delete(mirror_deletion);

    const mutations = [];
    const mutation_count = Math.min(20, Math.max(2, Math.floor(files / 5)));
    for (let index = 1; index <= mutation_count; index++) {
        const server_uri = `/server-${String(index).padStart(4, '0')}.bin`;
        const mirror_uri = `/mirror-${String(server_files + index).padStart(4, '0')}.bin`;
        if (expected.has(server_uri)) mutations.push({ root: source, uri: server_uri });
        if (expected.has(mirror_uri)) mutations.push({ root: target, uri: mirror_uri });
    }

    const mutation_started = performance.now();
    await Promise.all([
        FileSystem.rm(resolve_uri(source, server_deletion)),
        FileSystem.rm(resolve_uri(target, mirror_deletion)),
        in_batches(mutations, 10, async ({ root, uri }) => expected.set(uri, await write_random(root, uri))),
    ]);
    await wait_for(() => maps_match(server, mirror, expected, deleted));
    const mutation_duration = performance.now() - mutation_started;
    await Promise.all([
        verify_integrity(source, expected, deleted),
        verify_integrity(target, expected, deleted),
    ]);
    if (errors.length) throw new AggregateError(errors, 'Stress run emitted errors.');

    const megabytes = files * bytes / 1024 / 1024;
    console.log(JSON.stringify({
        initial_files: files,
        bytes_per_file: bytes,
        concurrency,
        directions: 2,
        live_overwrites: mutations.length,
        live_deletions: deleted.size,
        total_megabytes: Number(megabytes.toFixed(2)),
        initial_duration_ms: Number(initial_duration.toFixed(2)),
        initial_megabytes_per_second: Number((megabytes / (initial_duration / 1_000)).toFixed(2)),
        mutation_duration_ms: Number(mutation_duration.toFixed(2)),
        integrity: 'verified',
    }, null, 2));
} finally {
    await mirror?.destroy();
    await server?.destroy();
    await Promise.all([
        FileSystem.rm(source, { recursive: true, force: true }),
        FileSystem.rm(target, { recursive: true, force: true }),
    ]);
}
