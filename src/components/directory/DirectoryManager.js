import Crypto, { randomUUID } from 'node:crypto';
import FileSystemSync from 'node:fs';
import FileSystem from 'node:fs/promises';
import OS from 'node:os';
import Path from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import {
    canonicalize_uri,
    compare_entries,
    resolve_uri,
    validate_entry,
} from '../../utils/operators.js';

function operation_error(code, message) {
    const error = new Error(message);
    error.code = code;
    return error;
}

function uri_depth(uri) {
    let depth = 0;
    for (let index = 0; index < uri.length; index++)
        if (uri.charCodeAt(index) === 47) depth++;
    return depth;
}

function as_readable(data) {
    if (data == null) return Readable.from([]);
    if (data instanceof Readable || typeof data?.pipe === 'function') return data;
    if (typeof data?.getReader === 'function') return Readable.fromWeb(data);
    if (typeof data === 'string' || Buffer.isBuffer(data) || ArrayBuffer.isView(data))
        return Readable.from([data]);
    if (data instanceof ArrayBuffer) return Readable.from([Buffer.from(data)]);
    throw new TypeError('File content must be a string, Buffer, Node stream, or Web stream.');
}

let temporary_directory_promise;

function system_temporary_directory() {
    if (!temporary_directory_promise) {
        temporary_directory_promise = (async () => {
            try {
                const path = OS.tmpdir();
                if (typeof path !== 'string' || !path)
                    throw new Error('OS temporary directory is unavailable.');
                await FileSystem.access(path, FileSystemSync.constants.W_OK);
                return path;
            } catch {
                return undefined;
            }
        })();
    }
    return temporary_directory_promise;
}

/**
 * Applies validated repository mutations to a DirectoryMap's filesystem.
 *
 * File writes stream through a SHA-256/size verifier into the OS temporary
 * directory and replace the destination only after validation succeeds.
 * Per-URI serialization in DirectoryMap prevents overlapping commits.
 */
export default class DirectoryManager {
    #map;
    #supress_mutations;

    /**
     * @param {import('../../../index.js').DirectoryMap} map Map that owns the target root.
     * @param {boolean} [supress_mutations=true] Suppress exact watcher echoes produced by this manager.
     */
    constructor(map, supress_mutations = true) {
        this.#map = map;
        this.#supress_mutations = supress_mutations;
    }

    /** @protected @param {string} uri Repository URI. @returns {string} Safe absolute path below the map root. */
    _absolute_path(uri) {
        return resolve_uri(this.#map.path, canonicalize_uri(uri));
    }

    async _assert_safe_parent(path) {
        const root = Path.resolve(this.#map.path);
        const relative = Path.relative(root, Path.dirname(path));
        if (relative.startsWith('..') || Path.isAbsolute(relative))
            throw operation_error('INVALID_URI', 'Path escapes the synchronized root.');
        let cursor = root;
        for (const segment of relative.split(Path.sep)) {
            if (!segment) continue;
            cursor = Path.join(cursor, segment);
            try {
                const stats = await FileSystem.lstat(cursor);
                if (stats.isSymbolicLink())
                    throw operation_error('INVALID_URI', `Symbolic-link path component is not allowed: ${segment}`);
                if (!stats.isDirectory())
                    throw operation_error('TYPE_CONFLICT', `A parent path is not a directory: ${cursor}`);
            } catch (error) {
                if (error.code === 'ENOENT') break;
                throw error;
            }
        }
        await FileSystem.mkdir(Path.dirname(path), { recursive: true });
        const [real_root, real_parent] = await Promise.all([
            FileSystem.realpath(root),
            FileSystem.realpath(Path.dirname(path)),
        ]);
        const real_relative = Path.relative(real_root, real_parent);
        if (real_relative.startsWith('..') || Path.isAbsolute(real_relative))
            throw operation_error('INVALID_URI', 'Resolved path escapes the synchronized root.');
    }

    async _remove_type_conflict(path, desired_type) {
        try {
            const stats = await FileSystem.lstat(path);
            if (stats.isSymbolicLink())
                throw operation_error('INVALID_URI', 'Symbolic links cannot be synchronized.');
            if (
                (desired_type === 'file' && stats.isDirectory()) ||
                (desired_type === 'directory' && !stats.isDirectory())
            ) await FileSystem.rm(path, { recursive: true, force: true });
        } catch (error) {
            if (error.code !== 'ENOENT') throw error;
        }
    }

    async _temporary_path(destination, suffix = '.part') {
        const temporary = await system_temporary_directory();
        return temporary
            ? Path.join(temporary, `${randomUUID()}${suffix}`)
            : Path.join(Path.dirname(destination), `.${randomUUID()}${suffix}`);
    }

    async _rename_overwrite(source, destination) {
        try {
            await FileSystem.rename(source, destination);
        } catch (error) {
            if (!['EEXIST', 'EPERM'].includes(error.code)) throw error;
            await FileSystem.rm(destination, { recursive: true, force: true });
            await FileSystem.rename(source, destination);
        }
    }

    async _commit_temporary(temporary, destination, modified_at) {
        try {
            await this._rename_overwrite(temporary, destination);
            return;
        } catch (error) {
            if (error.code !== 'EXDEV') throw error;
        }

        const sibling = Path.join(Path.dirname(destination), `.${randomUUID()}.part`);
        try {
            await FileSystem.copyFile(temporary, sibling, FileSystemSync.constants.COPYFILE_EXCL);
            const seconds = modified_at / 1_000;
            await FileSystem.utimes(sibling, seconds, seconds);
            await this._rename_overwrite(sibling, destination);
        } finally {
            await FileSystem.rm(sibling, { force: true }).catch(() => undefined);
        }
    }

    /**
     * Reads an active file.
     *
     * @param {string} uri Repository URI.
     * @param {boolean} [stream=false] Return a Readable instead of buffering the file.
     * @returns {Promise<Buffer>|import('node:fs').ReadStream}
     */
    read(uri, stream = false) {
        const path = this._absolute_path(uri);
        return stream ? FileSystemSync.createReadStream(path) : FileSystem.readFile(path);
    }

    /**
     * Streams, verifies, and atomically commits file content and metadata.
     *
     * @param {string} uri Repository URI.
     * @param {string|Buffer|ArrayBuffer|ArrayBufferView|import('node:stream').Readable|ReadableStream} data File content.
     * @param {import('../../../index.js').FileEntry} entry Expected size, hash, and mtime.
     * @param {{signal?: AbortSignal, supress?: boolean, validate_before_commit?: () => boolean|Promise<boolean>}} [options={}] Transfer controls.
     * @returns {Promise<import('../../../index.js').MapRecord>}
     */
    async apply_file(uri, data, entry, options = {}) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type !== 'file') throw new TypeError('apply_file requires a file entry.');
        const maximum = this.#map.options.limits.max_file_size;
        if (maximum && entry.size > maximum)
            throw operation_error('SIZE_LIMIT', `File size ${entry.size} exceeds limit ${maximum}.`);

        return this.#map.run_serial(canonical, async () => {
            const path = this._absolute_path(canonical);
            await this._assert_safe_parent(path);
            const temporary = await this._temporary_path(path);
            const hash = Crypto.createHash('sha256');
            let size = 0;
            const verifier = new Transform({
                transform(chunk, encoding, callback) {
                    size += chunk.length;
                    if (maximum && size > maximum)
                        return callback(operation_error('SIZE_LIMIT', `Upload exceeds limit ${maximum}.`));
                    hash.update(chunk);
                    callback(null, chunk);
                },
            });

            try {
                // Never expose partial bytes at the repository path. Cross-device
                // moves stage one verified sibling before the final atomic rename.
                await pipeline(
                    as_readable(data),
                    verifier,
                    FileSystemSync.createWriteStream(temporary, { flags: 'wx', mode: 0o600 }),
                    options.signal ? { signal: options.signal } : {}
                );
                const digest = hash.digest('hex');
                if (size !== entry.size)
                    throw operation_error('INVALID_SIZE', `Expected ${entry.size} bytes but received ${size}.`);
                if (digest !== entry.sha256.toLowerCase())
                    throw operation_error('CHECKSUM', 'The streamed file failed SHA-256 validation.');
                if (options.validate_before_commit && !(await options.validate_before_commit()))
                    throw operation_error('STALE_MUTATION', 'The canonical entry changed during transfer.');

                const seconds = entry.modified_at / 1_000;
                await FileSystem.utimes(temporary, seconds, seconds);
                await this._remove_type_conflict(path, 'file');
                if (this.#supress_mutations || options.supress) this.#map.expect(canonical, entry);
                await this.#map.observe(path, () =>
                    this._commit_temporary(temporary, path, entry.modified_at)
                );
                const stats = await FileSystem.lstat(path);
                const committed = { ...entry, modified_at: Math.round(stats.mtimeMs) };
                if (this.#supress_mutations || options.supress) this.#map.expect(canonical, committed);
                return this.#map.commit_entry(canonical, committed, stats);
            } finally {
                await FileSystem.rm(temporary, { force: true }).catch(() => undefined);
            }
        });
    }

    /**
     * Updates only mtime when local content already matches size and SHA-256.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').FileEntry} entry Desired file metadata.
     * @returns {Promise<import('../../../index.js').MapRecord>}
     */
    async apply_metadata(uri, entry) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type !== 'file') throw new TypeError('apply_metadata requires a file entry.');
        return this.#map.run_serial(canonical, async () => {
            const current = this.#map.get_entry(canonical);
            if (
                current?.type !== 'file' ||
                current.size !== entry.size ||
                current.sha256 !== entry.sha256
            ) throw operation_error('CONTENT_REQUIRED', 'File content differs; metadata-only sync is unsafe.');
            const path = this._absolute_path(canonical);
            const seconds = entry.modified_at / 1_000;
            if (this.#supress_mutations) this.#map.expect(canonical, entry);
            await FileSystem.utimes(path, seconds, seconds);
            const stats = await FileSystem.lstat(path);
            const committed = {
                ...entry,
                modified_at: Math.round(stats.mtimeMs),
            };
            if (this.#supress_mutations) this.#map.expect(canonical, committed);
            return this.#map.commit_entry(canonical, committed, stats);
        });
    }

    /**
     * Creates or replaces a directory and applies its authoritative mtime.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').DirectoryEntry} entry Directory metadata.
     * @returns {Promise<import('../../../index.js').MapRecord>}
     */
    async apply_directory(uri, entry) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type !== 'directory') throw new TypeError('apply_directory requires a directory entry.');
        return this.#map.run_serial(canonical, async () => {
            const path = this._absolute_path(canonical);
            await this._assert_safe_parent(path);
            await this._remove_type_conflict(path, 'directory');
            if (this.#supress_mutations) this.#map.expect(canonical, entry);
            await FileSystem.mkdir(path, { recursive: true });
            const seconds = entry.modified_at / 1_000;
            await FileSystem.utimes(path, seconds, seconds);
            const stats = await FileSystem.lstat(path);
            const committed = {
                ...entry,
                modified_at: Math.round(stats.mtimeMs),
            };
            if (this.#supress_mutations) this.#map.expect(canonical, committed);
            return this.#map.commit_entry(canonical, committed, stats);
        });
    }

    /**
     * Applies a deletion while preserving descendants newer than a directory tombstone.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').TombstoneEntry} entry Deletion record.
     * @returns {Promise<import('../../../index.js').TombstoneEntry>}
     */
    async apply_tombstone(uri, entry) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type !== 'tombstone') throw new TypeError('apply_tombstone requires a tombstone.');
        return this.#map.run_serial(canonical, async () => {
            const path = this._absolute_path(canonical);
            let applied = entry;
            if (this.#supress_mutations) this.#map.expect(canonical, entry);
            try {
                if (entry.target === 'directory') {
                    const active = this.#map.active_entries_under(canonical);
                    const preserve_self = entry.include_self === false;
                    const entries = Object.entries(active);
                    const protected_uris = [];
                    const protected_ancestors = new Set();
                    let iterations = 0;
                    for (const [candidate, current] of entries) {
                        if (candidate !== canonical && compare_entries(entry, current) === 1) {
                            protected_uris.push(candidate);
                            let parent = Path.posix.dirname(candidate);
                            while (parent && parent !== '/') {
                                protected_ancestors.add(parent);
                                if (parent === canonical) break;
                                const next = Path.posix.dirname(parent);
                                if (next === parent) break;
                                parent = next;
                            }
                        }
                        if (++iterations % 2_048 === 0)
                            await new Promise((resolve) => setImmediate(resolve));
                    }
                    const removable = [];
                    iterations = 0;
                    for (const [candidate, current] of entries) {
                        if (++iterations % 2_048 === 0)
                            await new Promise((resolve) => setImmediate(resolve));
                        if (
                            (preserve_self && candidate === canonical) ||
                            compare_entries(entry, current) === 1
                        ) continue;
                        if (!protected_ancestors.has(candidate)) removable.push(candidate);
                    }
                    removable.sort((left, right) => uri_depth(right) - uri_depth(left));
                    if (preserve_self || protected_uris.length) {
                        applied = { ...entry, include_self: false };
                        for (const candidate of removable)
                            await FileSystem.rm(this._absolute_path(candidate), { recursive: true, force: true });
                    } else await FileSystem.rm(path, { recursive: true, force: true });
                } else await FileSystem.rm(path, { recursive: true, force: true });
                if (this.#supress_mutations && applied !== entry) this.#map.expect(canonical, applied);
                return this.#map.commit_tombstone(canonical, applied);
            } catch (error) {
                if (this.#supress_mutations) this.#map._depress(canonical);
                throw error;
            }
        });
    }

    /**
     * Creates an empty file or directory using the current system time.
     * Retained with its v2 name and argument order.
     *
     * @param {string} uri Repository URI.
     * @param {boolean} [is_directory=false] Create a directory instead of a file.
     * @returns {Promise<import('../../../index.js').MapRecord|undefined>}
     */
    async create(uri, is_directory = false) {
        if (this.#map.get(uri)) return;
        const modified_at = Date.now();
        if (is_directory) return this.apply_directory(uri, { type: 'directory', modified_at });
        const sha256 = Crypto.createHash('sha256').update('').digest('hex');
        return this.apply_file(uri, '', { type: 'file', modified_at, size: 0, sha256 });
    }

    /**
     * Writes buffered data using the v2-compatible convenience signature.
     *
     * @param {string} uri Repository URI.
     * @param {string|Buffer|ArrayBuffer|ArrayBufferView|import('node:stream').Readable} data Buffered or streamed file content.
     * @param {string} [sha256] Optional legacy MD5 or SHA-256 digest.
     * @param {boolean} [supress] Suppress the exact watcher echo.
     * @returns {Promise<import('../../../index.js').MapRecord>}
     */
    async write(uri, data, sha256, supress) {
        let buffer;
        if (typeof data === 'string') buffer = Buffer.from(data);
        else if (Buffer.isBuffer(data) || ArrayBuffer.isView(data))
            buffer = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
        else if (data instanceof ArrayBuffer) buffer = Buffer.from(data);
        if (!buffer && (data instanceof Readable || typeof data?.pipe === 'function')) {
            const temporary = await this._temporary_path(this._absolute_path(uri), '.legacy.part');
            const digest = Crypto.createHash('sha256');
            const legacy = sha256?.length === 32 ? Crypto.createHash('md5') : undefined;
            let size = 0;
            const hasher = new Transform({
                transform(chunk, encoding, callback) {
                    size += chunk.length;
                    digest.update(chunk);
                    legacy?.update(chunk);
                    callback(null, chunk);
                },
            });
            try {
                await pipeline(as_readable(data), hasher, FileSystemSync.createWriteStream(temporary, { flags: 'wx' }));
                const actual_sha256 = digest.digest('hex');
                const supplied = sha256?.toLowerCase();
                const actual_supplied_hash = legacy ? legacy.digest('hex') : actual_sha256;
                if (supplied && supplied !== actual_supplied_hash)
                    throw operation_error('CHECKSUM', 'The streamed file failed checksum validation.');
                return await this.apply_file(uri, FileSystemSync.createReadStream(temporary), {
                    type: 'file',
                    modified_at: Date.now(),
                    size,
                    sha256: actual_sha256,
                }, { supress });
            } finally {
                await FileSystem.rm(temporary, { force: true }).catch(() => undefined);
            }
        }
        if (!buffer) throw new TypeError('File content must be buffered data or a readable stream.');
        const digest = Crypto.createHash('sha256').update(buffer).digest('hex');
        if (sha256) {
            const supplied = sha256.toLowerCase();
            const actual = supplied.length === 32
                ? Crypto.createHash('md5').update(buffer).digest('hex')
                : digest;
            if (supplied !== actual) throw operation_error('CHECKSUM', 'The file failed checksum validation.');
        }
        return this.apply_file(
            uri,
            buffer,
            { type: 'file', modified_at: Date.now(), size: buffer.length, sha256: digest },
            { supress }
        );
    }

    /**
     * Backward-compatible buffered indirect write. Protocol streams use apply_file() with full metadata.
     *
     * @param {string} uri Repository URI.
     * @param {string|Buffer|import('node:stream').Readable} data Buffered or streamed file content.
     * @param {string} [sha256] Optional legacy MD5 or SHA-256 digest.
     * @param {boolean} [supress] Suppress the exact watcher echo.
     * @returns {Promise<import('../../../index.js').MapRecord>}
     */
    async indirect_write(uri, data, sha256, supress) {
        return this.write(uri, data, sha256, supress);
    }

    /**
     * Copies a file entry to a new URI and tombstones the old URI.
     *
     * @param {string} from_uri Existing file URI.
     * @param {string} to_uri Destination URI.
     * @param {boolean} [supress] Suppress exact watcher echoes.
     * @returns {Promise<import('../../../index.js').TombstoneEntry|undefined>}
     */
    async move(from_uri, to_uri, supress) {
        const data = await this.read(from_uri);
        const source = this.#map.get_entry(from_uri);
        if (source?.type !== 'file') throw operation_error('NOT_FOUND', 'Source file does not exist.');
        await this.apply_file(to_uri, data, source, { supress });
        return this.delete(from_uri, false, supress);
    }

    /**
     * Deletes an active entry and records an in-memory tombstone when enabled.
     *
     * @param {string} uri Repository URI.
     * @param {boolean} [is_directory] Optional v2 type hint; current metadata is used when omitted.
     * @param {boolean} [supress] Retained v2 argument; manager-level suppression is automatic.
     * @returns {Promise<import('../../../index.js').TombstoneEntry|undefined>}
     */
    async delete(uri, is_directory, supress) { // eslint-disable-line no-unused-vars
        const current = this.#map.get_entry(uri);
        if (!current) return;
        return this.apply_tombstone(uri, {
            type: 'tombstone',
            target: (is_directory ?? current.type === 'directory') ? 'directory' : 'file',
            deleted_at: Date.now(),
            include_self: true,
        });
    }
}
