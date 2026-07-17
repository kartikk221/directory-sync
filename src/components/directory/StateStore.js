import Path from 'node:path';
import { canonicalize_uri, entry_timestamp, validate_entry } from '../../utils/operators.js';

/**
 * In-memory store for active metadata and Server tombstones.
 *
 * DirectorySync deliberately persists no private state. Active entries are
 * rebuilt from the filesystem on startup, while tombstones live only for the
 * lifetime of the hosting Server process.
 */
export default class StateStore {
    #active = new Map();
    #log;
    #max_tombstones;
    #tombstones = new Map();

    /**
     * @param {import('../../../index.js').StateOptions} [options={}] In-memory tombstone limit.
     * @param {(code: string, message: string) => void} [log] Diagnostic callback.
     */
    constructor(options = {}, log = () => undefined) {
        this.#max_tombstones = options.max_tombstones ?? 10_000;
        if (!Number.isSafeInteger(this.#max_tombstones) || this.#max_tombstones < 0)
            throw new TypeError('state.max_tombstones must be a non-negative safe integer.');
        this.#log = log;
    }

    /** @param {string} uri Repository URI. @returns {import('../../../index.js').Entry|undefined} In-memory active metadata. */
    get_cached(uri) {
        return this.#active.get(canonicalize_uri(uri));
    }

    /**
     * Returns the newest exact tombstone, optionally restricted by deleted type.
     *
     * @param {string} uri Repository URI.
     * @param {'file'|'directory'} [target] Optional deleted entry type.
     * @returns {import('../../../index.js').TombstoneEntry|undefined}
     */
    get_tombstone(uri, target) {
        const canonical = canonicalize_uri(uri);
        if (target) return this.#tombstones.get(`${target}:${canonical}`)?.entry;
        const file = this.#tombstones.get(`file:${canonical}`)?.entry;
        const directory = this.#tombstones.get(`directory:${canonical}`)?.entry;
        if (!file) return directory;
        if (!directory) return file;
        return file.deleted_at > directory.deleted_at ? file : directory;
    }

    /** @param {string} uri Repository URI. @param {'file'|'directory'} target Deleted type. */
    get_tombstone_record(uri, target) {
        return this.#tombstones.get(`${target}:${canonicalize_uri(uri)}`);
    }

    /**
     * Finds the newest exact tombstone or ancestor directory tombstone covering a URI.
     *
     * @param {string} uri Repository URI.
     * @returns {import('../../../index.js').TombstoneEntry|undefined}
     */
    effective_tombstone(uri) {
        return this.effective_tombstone_record(uri)?.entry;
    }

    /**
     * Finds the newest effective tombstone together with the URI that owns it.
     *
     * @param {string} uri Repository URI.
     * @returns {{uri: string, entry: import('../../../index.js').TombstoneEntry}|undefined}
     */
    effective_tombstone_record(uri) {
        const canonical = canonicalize_uri(uri);
        const file = this.#tombstones.get(`file:${canonical}`);
        const directory = this.#tombstones.get(`directory:${canonical}`);
        let selected;
        for (const record of [file, directory]) {
            if (!record || record.entry.include_self === false) continue;
            if (!selected || record.entry.deleted_at > selected.entry.deleted_at) selected = record;
        }
        let cursor = Path.posix.dirname(canonical);
        while (cursor && cursor !== '/') {
            const candidate = this.#tombstones.get(`directory:${cursor}`);
            if (candidate && (!selected || candidate.entry.deleted_at > selected.entry.deleted_at))
                selected = candidate;
            const parent = Path.posix.dirname(cursor);
            cursor = parent === cursor ? '/' : parent;
        }
        return selected;
    }

    /** @param {string} uri Repository URI. @param {import('../../../index.js').FileEntry|import('../../../index.js').DirectoryEntry} entry Active metadata. */
    set_active(uri, entry) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        this.#active.set(canonical, entry);
    }

    /** @param {string} uri Repository URI. */
    remove_active(uri) {
        this.#active.delete(canonicalize_uri(uri));
    }

    /** @param {string} uri Repository URI prefix. */
    remove_active_tree(uri) {
        const canonical = canonicalize_uri(uri);
        for (const candidate of [...this.#active.keys()])
            if (candidate === canonical || candidate.startsWith(`${canonical}/`)) this.#active.delete(candidate);
    }

    /** @param {Iterable<[string, import('../../../index.js').Entry]>} entries Complete active snapshot. */
    replace_active(entries) {
        this.#active = new Map(entries);
    }

    /**
     * Adds a bounded tombstone while removing only deletion records it subsumes.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').TombstoneEntry} entry Deletion record.
     * @returns {Promise<import('../../../index.js').TombstoneEntry>}
     */
    async record_tombstone(uri, entry) {
        const canonical = canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type !== 'tombstone') throw new TypeError('Expected a tombstone entry.');

        const ancestor = this._ancestor_directory_tombstone(canonical);
        if (ancestor?.deleted_at >= entry.deleted_at) return ancestor;

        const key = `${entry.target}:${canonical}`;
        const exact = this.#tombstones.get(key);
        if (exact?.entry.deleted_at >= entry.deleted_at) return exact.entry;

        if (entry.target === 'directory') {
            for (const [candidate, value] of this.#tombstones) {
                if (
                    value.uri.startsWith(`${canonical}/`) &&
                    value.entry.deleted_at <= entry.deleted_at
                ) this.#tombstones.delete(candidate);
            }
        }

        const normalized = { ...entry, include_self: entry.include_self ?? true };
        this.#tombstones.delete(key);
        this.#tombstones.set(key, { uri: canonical, entry: normalized });
        this._enforce_limit();
        return normalized;
    }

    _ancestor_directory_tombstone(uri) {
        let selected;
        let cursor = Path.posix.dirname(uri);
        while (cursor && cursor !== '/') {
            const candidate = this.get_tombstone(cursor, 'directory');
            if (candidate && (!selected || candidate.deleted_at > selected.deleted_at)) selected = candidate;
            const parent = Path.posix.dirname(cursor);
            cursor = parent === cursor ? '/' : parent;
        }
        return selected;
    }

    _enforce_limit() {
        while (this.#tombstones.size > this.#max_tombstones) {
            let oldest_key;
            let oldest;
            for (const [key, value] of this.#tombstones) {
                if (!oldest || value.entry.deleted_at < oldest.entry.deleted_at) {
                    oldest_key = key;
                    oldest = value;
                }
            }
            this.#tombstones.delete(oldest_key);
            this.#log('TOMBSTONE_EVICTED', oldest.uri);
        }
    }

    /** @returns {Map<string, import('../../../index.js').Entry>} Live active-entry map. */
    get active() { return this.#active; }

    /** @returns {Array<{uri: string, entry: import('../../../index.js').TombstoneEntry}>} Tombstones in insertion order. */
    get records() {
        const records = [];
        for (const { uri, entry } of this.#tombstones.values()) records.push({ uri, entry });
        return records;
    }

    /**
     * Backward-compatible Map view. When both entry types were deleted at one
     * path, the newest tombstone occupies the legacy URI key.
     */
    get tombstones() {
        const output = new Map();
        for (const { uri, entry } of this.#tombstones.values()) {
            const current = output.get(uri);
            if (!current || entry_timestamp(entry) > entry_timestamp(current)) output.set(uri, entry);
        }
        return output;
    }
}
