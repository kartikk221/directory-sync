import Crypto from 'node:crypto';
import FileSystem from 'node:fs';
import AsyncFileSystem from 'node:fs/promises';
import Path from 'node:path';

/** @param {unknown} value @returns {boolean} Whether value has a plain or null prototype. */
function is_plain_object(value) {
    if (value === null || typeof value !== 'object') return false;
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
}

/**
 * Deeply clones defaults and merges caller-provided options without mutating either input.
 * Arrays are copied and non-plain values retain identity.
 *
 * @param {object} defaults Default option tree.
 * @param {object} [provided={}] Caller overrides.
 * @returns {object}
 */
function merge_options(defaults, provided = {}) {
    if (!is_plain_object(defaults) || !is_plain_object(provided))
        throw new TypeError('merge_options(defaults, provided) requires plain objects.');

    const output = {};
    for (const [key, value] of Object.entries(defaults)) {
        if (Array.isArray(value)) output[key] = [...value];
        else if (is_plain_object(value)) output[key] = merge_options(value);
        else output[key] = value;
    }

    for (const [key, value] of Object.entries(provided)) {
        if (Array.isArray(value)) output[key] = [...value];
        else if (is_plain_object(value) && is_plain_object(output[key]))
            output[key] = merge_options(output[key], value);
        else if (is_plain_object(value)) output[key] = merge_options({}, value);
        else output[key] = value;
    }
    return output;
}

/**
 * Backward-compatible mutating merge retained for consumers of the v2 helper.
 *
 * @param {object} original Object mutated in place.
 * @param {object} target Overrides to merge.
 * @returns {object} The original object.
 */
function wrap_object(original, target) {
    const merged = merge_options(original, target);
    for (const key of Object.keys(original)) delete original[key];
    Object.assign(original, merged);
    return original;
}

/** @returns {Error & {code: string}} Standardized abort error used by queues and transfers. */
function abort_error() {
    const error = new Error('The operation was aborted.');
    error.name = 'AbortError';
    error.code = 'ABORT_ERR';
    return error;
}

/**
 * Abortable asynchronous delay.
 *
 * @param {number} [delay=0] Delay in milliseconds.
 * @param {AbortSignal} [signal] Optional cancellation signal.
 * @returns {Promise<void>}
 */
function async_wait(delay = 0, signal) {
    if (signal?.aborted) return Promise.reject(abort_error());
    return new Promise((resolve, reject) => {
        const timer = setTimeout(resolve, Math.max(0, delay));
        if (signal) {
            signal.addEventListener(
                'abort',
                () => {
                    clearTimeout(timer);
                    reject(abort_error());
                },
                { once: true }
            );
        }
    });
}

/** @param {string} name File name. @param {string} extension Extension with or without a dot. @returns {boolean} */
function match_extension(name, extension) {
    if (typeof name !== 'string' || typeof extension !== 'string') return false;
    return name.endsWith(extension.startsWith('.') ? extension : `.${extension}`);
}

/** @param {string} path @returns {string} Path with Windows separators normalized. */
function to_forward_slashes(path) {
    return path.split('\\').join('/');
}

/** @param {string} value @returns {string} Normalized leading-slash repository URI. */
function to_path_uri(value) {
    if (typeof value !== 'string') throw new TypeError('Path URI must be a string.');
    let uri = to_forward_slashes(value.trim());
    if (!uri.startsWith('/')) uri = `/${uri}`;
    uri = uri.replace(/\/{2,}/g, '/');
    if (uri.length > 1) uri = uri.replace(/\/$/, '');
    return uri;
}

/**
 * Validates and canonicalizes an untrusted repository URI.
 *
 * @param {string} value Candidate URI.
 * @param {{allow_root?: boolean}} [options] Validation exceptions for internal callers.
 * @returns {string}
 */
function canonicalize_uri(value, { allow_root = false } = {}) {
    if (typeof value !== 'string') throw new TypeError('URI must be a string.');
    if (value.includes('\0')) throw new Error('URI must not contain NUL bytes.');
    if (value.includes('\\')) throw new Error('URI must use forward slashes.');

    const uri = to_path_uri(value);
    const segments = uri.split('/').slice(1);
    if (segments.some((segment) => segment === '.' || segment === '..'))
        throw new Error('URI must not contain relative path segments.');
    if (!allow_root && uri === '/') throw new Error('The synchronized root is not a valid entry URI.');
    return uri;
}

/**
 * Resolves a repository URI while proving the result remains below root.
 *
 * @param {string} root Absolute synchronized root.
 * @param {string} uri Repository URI.
 * @param {{allow_root?: boolean}} [options] Canonicalization options.
 * @returns {string}
 */
function resolve_uri(root, uri, options) {
    const canonical = canonicalize_uri(uri, options);
    const absolute_root = Path.resolve(root);
    const absolute = Path.resolve(absolute_root, `.${canonical}`);
    const relative = Path.relative(absolute_root, absolute);
    if (relative.startsWith('..') || Path.isAbsolute(relative))
        throw new Error(`URI escapes the synchronized root: ${canonical}`);
    return absolute;
}

/** @param {string} path @param {{directory?: boolean}} [options] @returns {Promise<boolean>} */
async function is_accessible_path(path, { directory = false } = {}) {
    try {
        await AsyncFileSystem.access(path, FileSystem.constants.R_OK | FileSystem.constants.W_OK);
        if (directory) return (await AsyncFileSystem.lstat(path)).isDirectory();
        return true;
    } catch {
        return false;
    }
}

/** @param {string} path @returns {Promise<string>} Lowercase SHA-256 digest generated without buffering the file. */
function generate_sha256_hash(path) {
    return new Promise((resolve, reject) => {
        const hash = Crypto.createHash('sha256');
        const stream = FileSystem.createReadStream(path);
        stream.on('data', (chunk) => hash.update(chunk));
        stream.once('error', reject);
        stream.once('end', () => resolve(hash.digest('hex')));
    });
}

/** Compatibility alias. Protocol v5 uses SHA-256 despite the legacy name. */
const generate_md5_hash = generate_sha256_hash;

/** @template T @param {string} value @param {T} [fallback] @returns {unknown|T} */
function safe_json_parse(value, fallback) {
    try {
        return JSON.parse(value);
    } catch {
        return fallback;
    }
}

/** @param {import('../../index.js').Entry} [entry] @returns {number} Conflict timestamp or negative infinity. */
function entry_timestamp(entry) {
    if (!entry) return Number.NEGATIVE_INFINITY;
    return entry.type === 'tombstone' ? entry.deleted_at : entry.modified_at;
}

function validate_timestamp(value, name) {
    if (!Number.isSafeInteger(value) || value < 0)
        throw new TypeError(`${name} must be a non-negative safe integer.`);
}

/** @param {unknown} entry @returns {import('../../index.js').Entry} Validated wire entry. */
function validate_entry(entry) {
    if (!is_plain_object(entry)) throw new TypeError('Entry must be an object.');
    if (!['file', 'directory', 'tombstone'].includes(entry.type))
        throw new TypeError(`Unsupported entry type '${entry.type}'.`);

    if (entry.type === 'file') {
        validate_timestamp(entry.modified_at, 'entry.modified_at');
        if (!Number.isSafeInteger(entry.size) || entry.size < 0)
            throw new TypeError('entry.size must be a non-negative safe integer.');
        if (typeof entry.sha256 !== 'string' || !/^[a-f0-9]{64}$/i.test(entry.sha256))
            throw new TypeError('entry.sha256 must be a SHA-256 hexadecimal digest.');
    } else if (entry.type === 'directory') {
        validate_timestamp(entry.modified_at, 'entry.modified_at');
    } else {
        validate_timestamp(entry.deleted_at, 'entry.deleted_at');
        if (!['file', 'directory'].includes(entry.target))
            throw new TypeError("entry.target must be 'file' or 'directory'.");
        if (entry.include_self !== undefined && typeof entry.include_self !== 'boolean')
            throw new TypeError('entry.include_self must be a boolean.');
    }
    return entry;
}

/** @param {import('../../index.js').Entry} [left] @param {import('../../index.js').Entry} [right] @returns {boolean} */
function entries_equivalent(left, right) {
    if (!left || !right || left.type !== right.type) return false;
    if (left.type === 'file')
        return (
            left.modified_at === right.modified_at &&
            left.size === right.size &&
            left.sha256 === right.sha256
        );
    if (left.type === 'directory') return left.modified_at === right.modified_at;
    return (
        left.target === right.target &&
        left.deleted_at === right.deleted_at &&
        (left.include_self ?? true) === (right.include_self ?? true)
    );
}

/**
 * Compares canonical authority state with an incoming candidate.
 *
 * @param {import('../../index.js').Entry} [server_entry] Current authority/local entry.
 * @param {import('../../index.js').Entry} [incoming_entry] Candidate entry.
 * @returns {-1|0|1} 1 when incoming wins, -1 when current wins, or 0 when equivalent.
 */
function compare_entries(server_entry, incoming_entry) {
    if (!server_entry) return incoming_entry ? 1 : 0;
    if (!incoming_entry) return -1;
    if (entries_equivalent(server_entry, incoming_entry)) return 0;

    const server_time = entry_timestamp(server_entry);
    const incoming_time = entry_timestamp(incoming_entry);
    if (incoming_time > server_time) return 1;
    return -1; // The server/current authority wins exact timestamp ties.
}

/** @param {import('../../index.js').Entry} [entry] @returns {string} Stable watcher-suppression fingerprint. */
function fingerprint_entry(entry) {
    if (!entry) return '';
    if (entry.type === 'file')
        return `file:${entry.modified_at}:${entry.size}:${entry.sha256}`;
    if (entry.type === 'directory') return `directory:${entry.modified_at}`;
    return `tombstone:${entry.target}:${entry.deleted_at}:${entry.include_self ?? true}`;
}

/** @param {unknown} left @param {unknown} right @returns {boolean} Timing-safe equality for equal-length values. */
function constant_time_equal(left, right) {
    const left_buffer = Buffer.from(String(left));
    const right_buffer = Buffer.from(String(right));
    if (left_buffer.length !== right_buffer.length) return false;
    return Crypto.timingSafeEqual(left_buffer, right_buffer);
}

/** @param {string} string @returns {string} UTF-8 bytes encoded as hexadecimal. */
function ascii_to_hex(string) {
    return Buffer.from(string).toString('hex');
}

/** @param {string} string @returns {string} Hexadecimal bytes decoded as UTF-8. */
function hex_to_ascii(string) {
    return Buffer.from(string, 'hex').toString('utf8');
}

export {
    abort_error,
    ascii_to_hex,
    async_wait,
    canonicalize_uri,
    compare_entries,
    constant_time_equal,
    entries_equivalent,
    entry_timestamp,
    fingerprint_entry,
    generate_md5_hash,
    generate_sha256_hash,
    hex_to_ascii,
    is_accessible_path,
    is_plain_object,
    match_extension,
    merge_options,
    resolve_uri,
    safe_json_parse,
    to_forward_slashes,
    to_path_uri,
    validate_entry,
    wrap_object,
};
