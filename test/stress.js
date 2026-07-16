import FileSystem from 'node:fs/promises';
import Net from 'node:net';
import OS from 'node:os';
import Path from 'node:path';
import { Mirror, Server } from '../index.js';

const files = Number.parseInt(process.env.BENCHMARK_FILES || '100', 10);
const bytes = Number.parseInt(process.env.BENCHMARK_FILE_SIZE || String(64 * 1024), 10);
if (!Number.isSafeInteger(files) || files < 1) throw new TypeError('BENCHMARK_FILES must be positive.');
if (!Number.isSafeInteger(bytes) || bytes < 0) throw new TypeError('BENCHMARK_FILE_SIZE must be non-negative.');

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

const source = await FileSystem.mkdtemp(Path.join(OS.tmpdir(), 'directory-sync-benchmark-source-'));
const target = await FileSystem.mkdtemp(Path.join(OS.tmpdir(), 'directory-sync-benchmark-target-'));
const content = Buffer.alloc(bytes, 0x61);
let mirror;
let server;

try {
    await Promise.all(Array.from({ length: files }, (_, index) =>
        FileSystem.writeFile(Path.join(source, `${index}.bin`), content)
    ));
    const port = await available_port();
    server = new Server({ port, auth: 'benchmark' });
    server.on('error', (error) => console.error(error));
    await server.ready();
    await server.host('benchmark', source, { watcher: { awaitWriteFinish: false } });

    const started = performance.now();
    mirror = new Mirror('benchmark', {
        path: target,
        hostname: '127.0.0.1',
        port,
        auth: 'benchmark',
        watcher: { awaitWriteFinish: false },
    });
    mirror.on('error', (error) => console.error(error));
    await mirror.ready();
    const duration = performance.now() - started;
    const megabytes = files * bytes / 1024 / 1024;
    console.log(JSON.stringify({
        files,
        bytes_per_file: bytes,
        total_megabytes: Number(megabytes.toFixed(2)),
        duration_ms: Number(duration.toFixed(2)),
        megabytes_per_second: Number((megabytes / (duration / 1_000)).toFixed(2)),
    }, null, 2));
} finally {
    await mirror?.destroy();
    await server?.destroy();
    await Promise.all([
        FileSystem.rm(source, { recursive: true, force: true }),
        FileSystem.rm(target, { recursive: true, force: true }),
    ]);
}
