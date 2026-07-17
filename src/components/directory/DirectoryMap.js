import EventEmitter from 'node:events';
import FileSystem from 'node:fs/promises';
import OS from 'node:os';
import Path from 'node:path';
import Chokidar from 'chokidar';
import StateStore from './StateStore.js';
import { KeyedQueue, TaskQueue } from '../../utils/queues.js';
import {
    canonicalize_uri,
    compare_entries,
    entries_equivalent,
    fingerprint_entry,
    generate_sha256_hash,
    is_accessible_path,
    match_extension,
    merge_options,
    resolve_uri,
    to_forward_slashes,
    to_path_uri,
    validate_entry,
} from '../../utils/operators.js';

const DEFAULT_OPTIONS = {
    path: '',
    filters: { keep: {}, ignore: {} },
    watcher: {
        alwaysStat: true,
        usePolling: false,
        awaitWriteFinish: { pollInterval: 100, stabilityThreshold: 500 },
        followSymlinks: false,
    },
    limits: { max_file_size: 100 * 1024 * 1024 },
    state: { max_tombstones: 10_000 },
    hashing: { max_concurrent: Math.min(4, OS.availableParallelism?.() || OS.cpus().length || 1) },
};

function normalize_filter(filter, name) {
    if (filter == null) return {};
    if (typeof filter === 'function') return filter; // v2 compatibility: evaluated by the owning process.
    if (typeof filter !== 'object' || Array.isArray(filter))
        throw new TypeError(`filters.${name} must be a function or a declarative filter object.`);
    const output = {};
    for (const key of ['files', 'directories', 'extensions']) {
        if (filter[key] === undefined) continue;
        if (!Array.isArray(filter[key]) || filter[key].some((value) => typeof value !== 'string'))
            throw new TypeError(`filters.${name}.${key} must be an array of strings.`);
        output[key] = [...filter[key]];
    }
    return output;
}

function compile_filter(filter) {
    if (typeof filter === 'function') return filter;
    const files = (filter.files || []).map(to_path_uri);
    const directories = (filter.directories || []).map(to_path_uri);
    const extensions = filter.extensions || [];
    if (!files.length && !directories.length && !extensions.length) return undefined;
    return (uri, stats, strict = true) => {
        const is_directory = stats.isDirectory();
        const in_directory = directories.some(
            (candidate) => uri === candidate || uri.startsWith(`${candidate}/`) || uri.endsWith(candidate)
        );
        if (in_directory) return true;
        if (is_directory) {
            if (strict) return false;
            return (
                Boolean(extensions.length) ||
                directories.some((candidate) => candidate.startsWith(`${uri}/`)) ||
                files.some((candidate) => candidate.startsWith(`${uri}/`))
            );
        }
        if (files.some((candidate) => uri === candidate || uri.endsWith(candidate))) return true;
        const name = Path.posix.basename(uri);
        if (extensions.some((extension) => match_extension(name, extension))) return true;
        return strict ? false : !files.length && !extensions.length;
    };
}

function stats_adapter(type) {
    return {
        isDirectory: () => type === 'directory',
        isFile: () => type === 'file',
        isSymbolicLink: () => false,
    };
}

function uri_depth(uri) {
    let depth = 0;
    for (let index = 0; index < uri.length; index++)
        if (uri.charCodeAt(index) === 47) depth++;
    return depth;
}

/**
 * Tracks a synchronized root and its content hashes directly from the filesystem.
 *
 * Chokidar reports that a path may have changed; it does not provide a serialized
 * transaction log. DirectoryMap therefore revalidates filesystem state, serializes
 * work by canonical URI, and records only stable entries in memory.
 *
 * @extends EventEmitter
 */
export default class DirectoryMap extends EventEmitter {
    #destroyed = false;
    #directories = {};
    #expected = new Map();
    #files = {};
    #hash_queue;
    #ignore_filter;
    #initializing = true;
    #keep_filter;
    #legacy_supressions = Object.create(null);
    #observations = new Map();
    #options;
    #path;
    #pending = new Set();
    #ready_promise;
    #recent_directory_deletions = new Map();
    #serial = new KeyedQueue();
    #sorted_active_uris;
    #state;
    #watcher;

    /**
     * Creates a live map for a local directory.
     *
     * @param {import('../../../index.js').DirectoryMapOptions} options Mapping, filtering, watcher, and state options.
     */
    constructor(options) {
        super();
        if (!options || typeof options !== 'object')
            throw new TypeError('new DirectoryMap(options) -> options must be an object.');
        if (typeof options.path !== 'string' || !options.path)
            throw new TypeError('new DirectoryMap(options.path) -> path must be a non-empty string.');
        if (options.watcher?.followSymlinks === true)
            throw new TypeError('DirectorySync does not follow symbolic links; watcher.followSymlinks must be false.');

        this.#options = merge_options(DEFAULT_OPTIONS, options);
        delete this.#options.state.path;
        this.#options.filters.keep = normalize_filter(this.#options.filters.keep, 'keep');
        this.#options.filters.ignore = normalize_filter(this.#options.filters.ignore, 'ignore');
        this.#path = to_forward_slashes(Path.resolve(this.#options.path));
        this.#options.path = this.#path;
        this.#options.watcher.alwaysStat = true;
        // Match Chokidar's documented backend behavior: atomic coalescing is
        // useful with fs.watch, while polling already supplies stable snapshots.
        if (options.watcher?.atomic === undefined)
            this.#options.watcher.atomic = !this.#options.watcher.usePolling;
        this.#options.watcher.followSymlinks = false;
        const concurrency = this.#options.hashing.max_concurrent;
        if (!Number.isSafeInteger(concurrency) || concurrency < 1)
            throw new TypeError('hashing.max_concurrent must be a positive safe integer.');
        this.#hash_queue = new TaskQueue(concurrency);
        this.#state = new StateStore(this.#options.state, (code, message) =>
            this.emit('log', code, message)
        );
        this.#ready_promise = this._initialize();
        this.#ready_promise.catch((error) => this.emit('error', error));
    }

    async _initialize() {
        if (!(await is_accessible_path(this.#path, { directory: true })))
            throw new Error(`Unable to access synchronized directory: ${this.#path}`);
        this.#keep_filter = compile_filter(this.#options.filters.keep);
        this.#ignore_filter = compile_filter(this.#options.filters.ignore);
        const user_ignored = this.#options.watcher.ignored;
        this.#options.watcher.ignored = (path, stats) => {
            const absolute = to_forward_slashes(Path.resolve(path));
            if (stats?.isSymbolicLink?.()) return true;
            if (absolute === this.#path) return false;
            if (typeof user_ignored === 'function' && user_ignored(path, stats)) return true;
            if (!stats) return false;
            const uri = this.relative_uri(absolute);
            if (this.#ignore_filter?.(uri, stats, true)) return true;
            return Boolean(this.#keep_filter && !this.#keep_filter(uri, stats, false));
        };

        this.#watcher = Chokidar.watch(this.#path, this.#options.watcher);
        this.#watcher.on('error', (error) => this.emit('error', error));
        this.#watcher.on('addDir', (path, stats) => this._track(this._on_directory(path, stats, false)));
        this.#watcher.on('add', (path, stats) => {
            this._observe_path(path);
            this._track(this._on_file(path, stats, false));
        });
        this.#watcher.on('change', (path, stats) => {
            this._observe_path(path);
            this._track(this._on_file(path, stats, true));
        });
        this.#watcher.on('unlink', (path) => this._track(this._on_delete(path, 'file')));
        this.#watcher.on('unlinkDir', (path) => this._track(this._on_delete(path, 'directory')));
        await new Promise((resolve, reject) => {
            this.#watcher.once('ready', resolve);
            this.#watcher.once('error', reject);
        });
        while (this.#pending.size) await Promise.allSettled([...this.#pending]);

        const current = [];
        for (const records of [this.#directories, this.#files])
            for (const [uri, record] of Object.entries(records)) current.push([uri, record.entry]);
        this.#state.replace_active(current);
        this.#initializing = false;
    }

    _track(promise) {
        this.#pending.add(promise);
        promise.catch((error) => this.emit('error', error)).finally(() => this.#pending.delete(promise));
    }

    /**
     * Converts an absolute path under this map into its canonical wire URI.
     *
     * @param {string} path Absolute or root-relative filesystem path.
     * @returns {string} Canonical forward-slash URI.
     */
    relative_uri(path) {
        const relative = to_forward_slashes(Path.relative(this.#path, Path.resolve(path)));
        return canonicalize_uri(`/${relative}`);
    }

    /**
     * Resolves a canonical URI without allowing it to escape the synchronized root.
     *
     * @param {string} uri Repository URI.
     * @returns {string} Absolute filesystem path.
     */
    resolve(uri) {
        return resolve_uri(this.#path, uri);
    }

    /**
     * Tests whether the configured keep/ignore filters permit an entry.
     *
     * @param {string} uri Repository URI.
     * @param {'file'|'directory'} [type='file'] Candidate entry type.
     * @returns {boolean}
     */
    allows(uri, type = 'file') {
        const canonical = canonicalize_uri(uri);
        const stats = stats_adapter(type);
        return (
            !this.#ignore_filter?.(canonical, stats, true) &&
            (!this.#keep_filter || this.#keep_filter(canonical, stats, false))
        );
    }

    async _stable_file_entry(path, stats) {
        // Hashing is optimistic: reuse a hash only when size and mtime match the
        // in-memory cache, otherwise verify that both remained stable around the
        // streamed SHA-256 pass. A type transition is retried by its own event.
        let before = stats;
        for (let attempt = 0; attempt < 3; attempt++) {
            if (!before || !before.isFile()) before = await FileSystem.lstat(path);
            if (before.isSymbolicLink() || !before.isFile()) return undefined;
            const modified_at = Math.round(before.mtimeMs);
            const cached = this.#state.get_cached(this.relative_uri(path));
            let sha256;
            if (
                cached?.type === 'file' &&
                cached.size === before.size &&
                cached.modified_at === modified_at
            ) sha256 = cached.sha256;
            else {
                try {
                    sha256 = await this.#hash_queue.run(() => generate_sha256_hash(path));
                } catch (error) {
                    if (['EISDIR', 'ENOENT'].includes(error.code)) return undefined;
                    throw error;
                }
            }
            const after = await FileSystem.lstat(path);
            if (after.size === before.size && Math.round(after.mtimeMs) === modified_at)
                return { type: 'file', modified_at, size: after.size, sha256 };
            before = after;
        }
        throw new Error(`File changed repeatedly while hashing: ${path}`);
    }

    _record(uri, path, stats, entry) {
        return {
            uri,
            path: to_forward_slashes(path),
            entry,
            stats: {
                md5: entry.type === 'file' ? entry.sha256 : '',
                sha256: entry.type === 'file' ? entry.sha256 : '',
                size: entry.type === 'file' ? entry.size : stats?.size || 0,
                created_at: Math.round(stats?.birthtimeMs ?? entry.modified_at),
                modified_at: entry.modified_at,
            },
        };
    }

    _consume_expected(uri, entry) {
        // Filesystem mutations performed by DirectoryManager also generate
        // Chokidar events. Fingerprints suppress only the exact expected echo;
        // unrelated user changes are never hidden by a counter.
        const expected = this.#expected.get(uri);
        if (!expected) return false;
        this.#expected.delete(uri);
        return expected.expires_at >= Date.now() && expected.fingerprint === fingerprint_entry(entry);
    }

    async _on_directory(path, stats, changed) {
        if (to_forward_slashes(Path.resolve(path)) === this.#path || this.#destroyed) return;
        const uri = this.relative_uri(path);
        await this.#serial.run(uri, async () => {
            stats ||= await FileSystem.lstat(path);
            if (stats.isSymbolicLink() || !stats.isDirectory() || !this.allows(uri, 'directory')) return;
            const entry = { type: 'directory', modified_at: Math.round(stats.mtimeMs) };
            const previous = this.get_entry(uri);
            this.commit_entry(uri, entry, stats);
            if (this.#initializing || this._consume_expected(uri, entry) || entries_equivalent(previous, entry)) return;
            const record = this.get(uri);
            if (this._depress(uri, 'directory_create')) return;
            this.emit('directory_create', uri, record);
            this.emit('mutation', uri, entry, previous);
        });
    }

    async _on_file(path, stats, changed) {
        if (this.#destroyed) return;
        stats ||= await FileSystem.lstat(path);
        if (stats.isDirectory()) return this._on_directory(path, stats, true);
        const uri = this.relative_uri(path);
        await this.#serial.run(uri, async () => {
            if (!this.allows(uri, 'file')) return;
            const entry = await this._stable_file_entry(path, stats);
            if (!entry) return;
            const previous = this.get_entry(uri);
            const record = this._record(uri, path, stats, entry);
            if (this.#options.limits.max_file_size && entry.size > this.#options.limits.max_file_size) {
                if (this.#files[uri] || this.#directories[uri]) this.#sorted_active_uris = undefined;
                delete this.#files[uri];
                delete this.#directories[uri];
                this.#state.remove_active(uri);
                this.emit('file_size_limit', uri, record);
                return;
            }
            this.commit_entry(uri, entry, stats);
            this.emit(`md5_change:${uri}`, this.get(uri));
            if (this.#initializing || this._consume_expected(uri, entry) || entries_equivalent(previous, entry)) return;
            const event = previous?.type === 'file' || changed ? 'file_change' : 'file_create';
            if (this._depress(uri, event)) return;
            this.emit(event, uri, this.get(uri));
            this.emit('mutation', uri, entry, previous);
        });
    }

    _covered_deletion_time(uri) {
        const now = Date.now();
        for (const [parent, value] of this.#recent_directory_deletions) {
            if (now - value > 1_000) this.#recent_directory_deletions.delete(parent);
            else if (uri.startsWith(`${parent}/`)) return value;
        }
        return now;
    }

    async _on_delete(path, target) {
        if (this.#destroyed || to_forward_slashes(Path.resolve(path)) === this.#path) return;
        const uri = this.relative_uri(path);
        await this.#serial.run(uri, async () => {
            // Chokidar's atomic option intentionally delays unlink by 100 ms and
            // polling observes at its own interval. A queued unlink may therefore
            // arrive after recreation. Events are notifications, so disk is the
            // final truth: re-arm an existing path and never mint a false tombstone.
            try {
                await FileSystem.lstat(path);
                if (!this.#destroyed) this.#watcher.unwatch(path).add(path);
                return;
            } catch (error) {
                if (error.code !== 'ENOENT') throw error;
            }
            const previous = this.get_entry(uri);
            if (!previous) return;
            let stale_type = false;
            if (previous.type !== target) {
                target = previous.type;
                stale_type = true;
            }
            const expected = this._expected_tombstone(uri, target);
            const base = expected?.entry.deleted_at ??
                (target === 'directory' ? Date.now() : this._covered_deletion_time(uri));
            const deleted_at = Math.max(
                base,
                Math.min(Number.MAX_SAFE_INTEGER, previous.modified_at + 1)
            );
            if (target === 'directory') this.#recent_directory_deletions.set(uri, deleted_at);
            const entry = { type: 'tombstone', target, deleted_at, include_self: true };
            const applied = await this.commit_tombstone(uri, entry);
            if (stale_type && !this.#destroyed) this.#watcher.unwatch(path).add(path);
            if (expected?.uri === uri) this.#expected.delete(uri);
            if (this.#initializing || expected) return;
            const event = target === 'directory' ? 'directory_delete' : 'file_delete';
            if (this._depress(uri, event)) return;
            this.emit(event, uri);
            this.emit('mutation', uri, applied, previous);
        });
    }

    _expected_tombstone(uri, target) {
        const now = Date.now();
        let cursor = uri;
        while (cursor && cursor !== '/') {
            const expected = this.#expected.get(cursor);
            if (expected?.expires_at < now) this.#expected.delete(cursor);
            else if (
                expected?.entry.type === 'tombstone' &&
                (cursor === uri ? expected.entry.target === target : expected.entry.target === 'directory')
            ) return { uri: cursor, entry: expected.entry };
            const parent = Path.posix.dirname(cursor);
            cursor = parent === cursor ? '/' : parent;
        }
        return undefined;
    }

    /**
     * Registers the exact watcher event expected from an internal disk mutation.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').Entry} entry Expected canonical entry.
     * @param {number} [ttl=5000] Maximum suppression lifetime in milliseconds.
     * @returns {void}
     */
    expect(uri, entry, ttl = 5_000) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        this.#expected.set(canonical, {
            entry,
            fingerprint: fingerprint_entry(entry),
            expires_at: Date.now() + ttl,
        });
    }

    _observe_path(path) {
        const absolute = Path.resolve(path);
        const observations = this.#observations.get(absolute);
        if (!observations) return;
        for (const complete of [...observations]) complete();
    }

    /**
     * Runs a filesystem mutation while observing its high-level Chokidar echo.
     * A raw backend event can precede native watcher re-registration after an
     * atomic replacement, so only add/change or the bounded fallback completes
     * this barrier before an immediate user edit is allowed to follow.
     *
     * @template T
     * @param {string} path Absolute filesystem path.
     * @param {() => T|Promise<T>} handler Filesystem mutation.
     * @returns {Promise<T>}
     */
    async observe(path, handler) {
        if (this.#destroyed) return handler();
        const absolute = Path.resolve(path);
        let complete;
        const observed = new Promise((resolve) => {
            let timer;
            complete = () => {
                clearTimeout(timer);
                const observations = this.#observations.get(absolute);
                observations?.delete(complete);
                if (!observations?.size) this.#observations.delete(absolute);
                resolve();
            };
            let observations = this.#observations.get(absolute);
            if (!observations) this.#observations.set(absolute, (observations = new Set()));
            observations.add(complete);
            const stability = this.#options.watcher.awaitWriteFinish?.stabilityThreshold || 0;
            timer = setTimeout(complete, Math.max(100, stability + 100));
            timer.unref?.();
        });
        try {
            const output = await handler();
            await observed;
            // Chokidar can emit before every backend has armed its native handle.
            await new Promise((resolve) => setTimeout(resolve, 10));
            return output;
        } catch (error) {
            complete();
            throw error;
        }
    }

    /**
     * Backward-compatible misspelled event suppression method retained from v2.
     * Internal writes use expect() fingerprints, while explicit callers keep
     * the original event/count behavior and return shape.
     *
     * @param {string} uri Repository URI.
     * @param {string} [event] V2 event name to suppress.
     * @param {number} [amount=1] Number of matching events to suppress.
     * @returns {{amount: number, updated_at: number}|undefined} Legacy suppression record.
     */
    supress(uri, event, amount = 1) {
        if (typeof event === 'string') {
            if (uri.startsWith('temporary://')) return undefined;
            const canonical = canonicalize_uri(uri);
            const key = `${event}:${canonical}`;
            const existing = this.#legacy_supressions[key];
            if (existing) {
                existing.amount += amount;
                existing.updated_at = Date.now();
            } else this.#legacy_supressions[key] = { amount, updated_at: Date.now() };
            return this.#legacy_supressions[key];
        }
        const entry = this.get_entry(uri);
        if (entry) this.expect(uri, entry);
        return undefined;
    }

    /**
     * Removes a watcher expectation. Retained for v2 code that called the legacy helper.
     *
     * @param {string} uri Repository URI.
     * @param {string} [event] Ignored legacy event name.
     * @param {number} [amount=1] Ignored legacy count.
     * @returns {boolean}
     */
    _depress(uri, event, amount = 1) {
        if (typeof event !== 'string') return this.#expected.delete(uri);
        const key = `${event}:${canonicalize_uri(uri)}`;
        const suppression = this.#legacy_supressions[key];
        if (!suppression) return false;
        suppression.amount -= amount;
        if (suppression.amount < 1) delete this.#legacy_supressions[key];
        return true;
    }

    /** @param {number} [max_age_ms=1000] Maximum legacy suppression age. @returns {void} */
    _cleanup_supressions(max_age_ms = 1_000) {
        const now = Date.now();
        for (const [uri, value] of this.#expected) if (value.expires_at < now) this.#expected.delete(uri);
        for (const [key, value] of Object.entries(this.#legacy_supressions))
            if (now - value.updated_at > max_age_ms) delete this.#legacy_supressions[key];
    }

    /**
     * Commits already-validated active metadata to the in-memory maps.
     * This method does not mutate the filesystem.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').FileEntry|import('../../../index.js').DirectoryEntry} entry Active entry.
     * @param {import('node:fs').Stats} [stats] Current filesystem stats.
     * @returns {import('../../../index.js').MapRecord}
     */
    commit_entry(uri, entry, stats) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type === 'tombstone') throw new TypeError('commit_entry requires an active entry.');
        const known = this.#directories[canonical] || this.#files[canonical];
        const path = this.resolve(canonical);
        const record = this._record(canonical, path, stats, entry);
        if (entry.type === 'file') {
            delete this.#directories[canonical];
            this.#files[canonical] = record;
        } else {
            delete this.#files[canonical];
            this.#directories[canonical] = record;
        }
        if (!known) this.#sorted_active_uris = undefined;
        this.#state.set_active(canonical, entry);
        return record;
    }

    /**
     * Commits a deletion and removes only active entries not newer than it.
     * Directory tombstones become non-self tombstones when a newer descendant
     * must keep the containing directory alive.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').TombstoneEntry} entry Deletion record.
     * @returns {Promise<import('../../../index.js').TombstoneEntry>}
     */
    async commit_tombstone(uri, entry) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type !== 'tombstone') throw new TypeError('commit_tombstone requires a tombstone.');
        if (entry.target === 'directory') {
            let protected_descendant = false;
            for (const records of [this.#files, this.#directories]) {
                for (const candidate of Object.keys(records)) {
                    if (candidate !== canonical && !candidate.startsWith(`${canonical}/`)) continue;
                    if (candidate === canonical && entry.include_self === false) continue;
                    if (compare_entries(entry, records[candidate].entry) === 1) {
                        protected_descendant ||= candidate !== canonical;
                        continue;
                    }
                    delete records[candidate];
                    this.#state.remove_active(candidate);
                }
            }
            if (protected_descendant) entry = { ...entry, include_self: false };
        } else {
            delete this.#files[canonical];
            delete this.#directories[canonical];
            this.#state.remove_active(canonical);
        }
        this.#sorted_active_uris = undefined;
        return this.#state.record_tombstone(canonical, entry);
    }

    /**
     * Returns active entries at or below a URI for directory-deletion planning.
     *
     * @param {string} uri Repository URI.
     * @returns {Record<string, import('../../../index.js').FileEntry|import('../../../index.js').DirectoryEntry>}
     */
    active_entries_under(uri) {
        const canonical = canonicalize_uri(uri);
        const output = {};
        for (const records of [this.#directories, this.#files])
            for (const [candidate, record] of Object.entries(records))
                if (candidate === canonical || candidate.startsWith(`${canonical}/`))
                    output[candidate] = record.entry;
        return output;
    }

    /**
     * Runs a mutation after earlier mutations for the same URI.
     *
     * @template T
     * @param {string} uri Repository URI used as the serialization key.
     * @param {() => T|Promise<T>} handler Work to perform.
     * @returns {Promise<T>}
     */
    run_serial(uri, handler) {
        return this.#serial.run(canonicalize_uri(uri), handler);
    }

    /** @param {string} uri Repository URI. @returns {import('../../../index.js').MapRecord|undefined} Active map record. */
    get(uri) {
        const canonical = canonicalize_uri(uri);
        return this.#directories[canonical] || this.#files[canonical];
    }

    /** @param {string} uri Repository URI. @returns {import('../../../index.js').FileEntry|import('../../../index.js').DirectoryEntry|undefined} Active entry metadata. */
    get_entry(uri) {
        return this.get(uri)?.entry;
    }

    /** @param {string} uri Repository URI. @returns {import('../../../index.js').TombstoneEntry|undefined} Exact tombstone. */
    get_tombstone(uri) {
        return this.#state.get_tombstone(canonicalize_uri(uri));
    }

    /**
     * Returns the newest effective active entry or covering tombstone for a URI.
     *
     * @param {string} uri Repository URI.
     * @returns {import('../../../index.js').Entry|undefined}
     */
    canonical_entry(uri) {
        const active = this.get_entry(uri);
        const tombstone = this.#state.effective_tombstone(uri);
        return compare_entries(active, tombstone) === 1 ? tombstone : active || tombstone;
    }

    /** @returns {Record<string, import('../../../index.js').Entry>} Sorted active filesystem manifest. */
    get manifest() {
        const output = {};
        if (!this.#sorted_active_uris) {
            const uris = [];
            for (const uri of Object.keys(this.#directories)) uris.push(uri);
            for (const uri of Object.keys(this.#files)) uris.push(uri);
            uris.sort((left, right) => uri_depth(left) - uri_depth(right) || left.localeCompare(right));
            this.#sorted_active_uris = uris;
        }
        for (const uri of this.#sorted_active_uris)
            output[uri] = (this.#directories[uri] || this.#files[uri]).entry;
        return output;
    }

    /** @returns {Array<{uri: string, entry: import('../../../index.js').TombstoneEntry}>} In-memory tombstones. */
    get tombstones() {
        return this.#state.records;
    }

    /**
     * V2-shaped metadata getter retained for applications that inspect it.
     * File hashes are SHA-256 even though the legacy slot previously held MD5.
     *
     * @returns {Record<string, Array<string|number>>}
     */
    get schema() {
        const output = {};
        for (const [uri, entry] of Object.entries(this.manifest)) {
            if (entry.type === 'file') output[uri] = [entry.sha256, entry.modified_at, entry.modified_at];
            else if (entry.type === 'directory') output[uri] = [entry.modified_at, entry.modified_at];
        }
        return output;
    }

    /** @returns {Promise<void>} Resolves after the initial filesystem scan. */
    ready() {
        return this.#ready_promise;
    }

    /**
     * Waits for watcher echoes and per-path work already observed by the map.
     *
     * @returns {Promise<void>}
     */
    async settled() {
        do {
            while (this.#pending.size) await Promise.allSettled([...this.#pending]);
            await this.#serial.idle();
            await new Promise((resolve) => setTimeout(resolve, 10));
        } while (this.#pending.size);
    }

    /** @returns {Promise<void>} Closes watcher and queues. Idempotent. */
    async destroy() {
        if (this.#destroyed) return;
        this.#destroyed = true;
        this.#hash_queue.close();
        this.#serial.close();
        for (const observations of this.#observations.values())
            for (const complete of [...observations]) complete();
        await this.#watcher?.close();
        while (this.#pending.size) await Promise.allSettled([...this.#pending]);
        await this.#serial.idle();
        this.removeAllListeners();
    }

    /** @returns {boolean} Whether destroy() has started. */
    get destroyed() { return this.#destroyed; }
    /** @returns {string} Absolute synchronized root. */
    get path() { return this.#path; }
    /** @returns {import('chokidar').FSWatcher} Underlying Chokidar watcher. */
    get watcher() { return this.#watcher; }
    /** @returns {import('../../../index.js').DirectoryMapOptions} Effective options. */
    get options() { return this.#options; }
    /** @returns {Record<string, import('../../../index.js').MapRecord>} Active directories keyed by URI. */
    get directories() { return this.#directories; }
    /** @returns {Record<string, import('../../../index.js').MapRecord>} Active files keyed by URI. */
    get files() { return this.#files; }
    /** @returns {Record<string, {amount: number, updated_at: number}>} Backward-compatible event suppression records. */
    get supressions() { return { ...this.#legacy_supressions }; }
    /** @returns {StateStore} Live in-memory metadata and tombstone store. */
    get state() { return this.#state; }
}
