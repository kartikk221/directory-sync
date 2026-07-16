import Crypto from 'node:crypto';
import FileSystem from 'node:fs/promises';
import OS from 'node:os';
import Path from 'node:path';
import Net from 'node:net';
import { setTimeout as wait } from 'node:timers/promises';

async function temporary_directory(t, name = 'directory-sync-') {
    const path = await FileSystem.mkdtemp(Path.join(OS.tmpdir(), name));
    t.after(() => FileSystem.rm(path, {
        recursive: true,
        force: true,
        maxRetries: 5,
        retryDelay: 20,
    }));
    return path;
}

async function write_file(root, uri, content, modified_at = Date.now()) {
    const path = Path.join(root, ...uri.split('/').filter(Boolean));
    await FileSystem.mkdir(Path.dirname(path), { recursive: true });
    await FileSystem.writeFile(path, content);
    const seconds = modified_at / 1_000;
    await FileSystem.utimes(path, seconds, seconds);
    return path;
}

async function wait_for(predicate, options = {}) {
    const timeout = options.timeout ?? 10_000;
    const interval = options.interval ?? 25;
    const started = Date.now();
    let error;
    while (Date.now() - started < timeout) {
        try {
            const value = await predicate();
            if (value) return value;
        } catch (caught) {
            error = caught;
        }
        await wait(interval);
    }
    if (error && !options.diagnostic) throw error;
    let message = options.message || `Condition was not met within ${timeout}ms.`;
    if (error) message += ` Last error: ${error.message}.`;
    if (options.diagnostic) message += ` ${JSON.stringify(await options.diagnostic())}`;
    throw new Error(message);
}

async function read_if_exists(path) {
    try {
        return await FileSystem.readFile(path, 'utf8');
    } catch (error) {
        if (error.code === 'ENOENT') return undefined;
        throw error;
    }
}

function sha256(content) {
    return Crypto.createHash('sha256').update(content).digest('hex');
}

function capture_errors(emitter) {
    const errors = [];
    emitter.on('error', (error) => errors.push(error));
    return errors;
}

function available_port() {
    return new Promise((resolve, reject) => {
        const server = Net.createServer();
        server.unref();
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
            const port = server.address().port;
            server.close((error) => error ? reject(error) : resolve(port));
        });
    });
}

export {
    available_port,
    capture_errors,
    read_if_exists,
    sha256,
    temporary_directory,
    wait_for,
    write_file,
};
