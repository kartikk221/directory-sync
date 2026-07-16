import FileSystem from 'node:fs/promises';
import Path from 'node:path';
import { randomUUID } from 'node:crypto';
import {
    canonicalize_uri,
    entry_timestamp,
    safe_json_parse,
    validate_entry,
} from '../../utils/operators.js';

const STATE_VERSION = 1;

/**
 * Durable metadata store for active entries and deletion tombstones.
 *
 * Tombstone writes are appended to an NDJSON journal before acknowledgment.
 * Periodic atomic snapshots bound recovery cost and replace the journal. Map
 * insertion order is the eviction order, so the oldest retained tombstone is
 * removed when max_tombstones is exceeded.
 */
export default class StateStore {
    #active = new Map();
    #closed = false;
    #journal_records = 0;
    #ledger_path;
    #log;
    #manifest_path;
    #max_tombstones;
    #sequence = 0;
    #snapshot_timer;
    #state_path;
    #tombstones = new Map();
    #write_chain = Promise.resolve();

    /**
     * @param {string} root Synchronized repository root.
     * @param {import('../../../index.js').StateOptions} [options={}] State path and tombstone limit.
     * @param {(code: string, message: string) => void} [log] State diagnostic callback.
     */
    constructor(root, options = {}, log = () => undefined) {
        this.#state_path = Path.resolve(options.path || Path.join(root, '.directory-sync'));
        this.#max_tombstones = options.max_tombstones ?? 10_000;
        if (!Number.isSafeInteger(this.#max_tombstones) || this.#max_tombstones < 0)
            throw new TypeError('state.max_tombstones must be a non-negative safe integer.');
        this.#manifest_path = Path.join(this.#state_path, 'manifest.json');
        this.#ledger_path = Path.join(this.#state_path, 'tombstones.ndjson');
        this.#log = log;
    }

    /** @returns {Promise<void>} Loads the snapshot and replays the tombstone journal. */
    async initialize() {
        await FileSystem.mkdir(this.tmp_path, { recursive: true });
        await this._load_manifest();
        await this._load_ledger();
        if (await this._enforce_limit()) await this.compact();
    }

    async _load_manifest() {
        let raw;
        try {
            raw = await FileSystem.readFile(this.#manifest_path, 'utf8');
        } catch (error) {
            if (error.code !== 'ENOENT') this.#log('STATE_REBUILT', `Unable to read state: ${error.message}`);
            return;
        }

        try {
            const parsed = JSON.parse(raw);
            if (parsed.version !== STATE_VERSION) throw new Error('Unsupported state version.');
            for (const [uri, entry] of Object.entries(parsed.active || {})) {
                canonicalize_uri(uri);
                validate_entry(entry);
                if (entry.type !== 'tombstone') this.#active.set(uri, entry);
            }
            for (const record of parsed.tombstones || []) this._apply_record(record);
            this.#sequence = Math.max(this.#sequence, parsed.sequence || 0);
        } catch (error) {
            this.#active.clear();
            this.#tombstones.clear();
            this.#log('STATE_REBUILT', `Discarded invalid state: ${error.message}`);
        }
    }

    async _load_ledger() {
        let raw;
        try {
            raw = await FileSystem.readFile(this.#ledger_path, 'utf8');
        } catch (error) {
            if (error.code !== 'ENOENT') this.#log('STATE_REBUILT', `Unable to read ledger: ${error.message}`);
            return;
        }
        // Each line is independently durable. A crash may truncate only the final
        // record, which can be ignored without discarding earlier deletions.
        for (const line of raw.split('\n')) {
            if (!line.trim()) continue;
            const record = safe_json_parse(line);
            if (!record) {
                this.#log('STATE_REBUILT', 'Ignored an incomplete tombstone journal record.');
                continue;
            }
            try {
                this._apply_record(record);
                this.#journal_records++;
            } catch (error) {
                this.#log('STATE_REBUILT', `Ignored invalid journal record: ${error.message}`);
            }
        }
    }

    _apply_record(record, validate = true) {
        const uri = canonicalize_uri(record.uri);
        if (record.op === 'drop') {
            this.#tombstones.delete(uri);
            return;
        }
        if (record.op !== 'put') throw new Error(`Unknown journal operation '${record.op}'.`);
        if (validate) validate_entry(record.entry);
        if (record.entry.type !== 'tombstone') throw new Error('Journal entries must be tombstones.');
        const sequence = Number.isSafeInteger(record.sequence) && record.sequence >= 0
            ? record.sequence
            : ++this.#sequence;
        this.#sequence = Math.max(this.#sequence, sequence);
        this.#tombstones.delete(uri);
        this.#tombstones.set(uri, { entry: record.entry, sequence });
    }

    _enqueue(handler) {
        this.#write_chain = this.#write_chain.catch(() => undefined).then(handler);
        return this.#write_chain;
    }

    async _append(record) {
        await FileSystem.appendFile(this.#ledger_path, `${JSON.stringify(record)}\n`, 'utf8');
        this.#journal_records++;
    }

    /** @param {string} uri Repository URI. @returns {import('../../../index.js').Entry|undefined} Cached active metadata. */
    get_cached(uri) {
        return this.#active.get(uri);
    }

    /** @param {string} uri Repository URI. @returns {import('../../../index.js').TombstoneEntry|undefined} Exact tombstone. */
    get_tombstone(uri) {
        return this.#tombstones.get(uri)?.entry;
    }

    /**
     * Finds the newest exact or ancestor-directory tombstone covering a URI.
     *
     * @param {string} uri Repository URI.
     * @returns {import('../../../index.js').TombstoneEntry|undefined}
     */
    effective_tombstone(uri) {
        const canonical = canonicalize_uri(uri);
        let selected;
        let cursor = canonical;
        while (cursor && cursor !== '/') {
            const record = this.#tombstones.get(cursor);
            if (record) {
                const is_self = cursor === canonical;
                if ((!is_self || record.entry.include_self !== false) && (!selected || record.entry.deleted_at > selected.deleted_at))
                    selected = record.entry;
            }
            const parent = Path.posix.dirname(cursor);
            cursor = parent === cursor ? '/' : parent;
        }
        return selected;
    }

    /**
     * Stores active metadata and retires an older exact tombstone on resurrection.
     * Directory tombstones remain with include_self=false so older descendants
     * still receive the historical deletion.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').FileEntry|import('../../../index.js').DirectoryEntry} entry Active metadata.
     * @returns {void}
     */
    set_active(uri, entry) {
        canonicalize_uri(uri);
        validate_entry(entry);
        this.#active.set(uri, entry);

        const exact = this.#tombstones.get(uri);
        if (exact && entry_timestamp(entry) > exact.entry.deleted_at) {
            if (exact.entry.target === 'directory') {
                exact.entry = { ...exact.entry, include_self: false };
                this._enqueue(() =>
                    this._append({ op: 'put', uri, entry: exact.entry, sequence: exact.sequence })
                );
            } else {
                this.#tombstones.delete(uri);
                this._enqueue(() => this._append({ op: 'drop', uri }));
            }
        }
        this.schedule_snapshot();
    }

    /** @param {string} uri Repository URI. @returns {void} */
    remove_active(uri) {
        this.#active.delete(uri);
        this.schedule_snapshot();
    }

    /** @param {string} uri Repository URI prefix. @returns {void} */
    remove_active_tree(uri) {
        const canonical = canonicalize_uri(uri);
        for (const candidate of [...this.#active.keys()]) {
            if (candidate === canonical || candidate.startsWith(`${canonical}/`))
                this.#active.delete(candidate);
        }
        this.schedule_snapshot();
    }

    /** @param {Iterable<[string, import('../../../index.js').Entry]>} entries Complete active snapshot. @returns {void} */
    replace_active(entries) {
        this.#active = new Map(entries);
        this.schedule_snapshot();
    }

    /**
     * Durably records a deletion unless an equal/newer covering deletion exists.
     *
     * @param {string} uri Repository URI.
     * @param {import('../../../index.js').TombstoneEntry} entry Deletion record.
     * @returns {Promise<import('../../../index.js').TombstoneEntry>}
     */
    async record_tombstone(uri, entry) {
        canonicalize_uri(uri);
        validate_entry(entry);
        if (entry.type !== 'tombstone') throw new TypeError('Expected a tombstone entry.');

        const exact = this.get_tombstone(uri);
        if (exact && exact.deleted_at >= entry.deleted_at) return exact;
        const ancestor = this.effective_tombstone(uri);
        if (ancestor && ancestor.deleted_at >= entry.deleted_at) return ancestor;

        let compact = false;
        if (entry.target === 'directory') {
            for (const candidate of [...this.#tombstones.keys()]) {
                if (candidate.startsWith(`${uri}/`)) {
                    this.#tombstones.delete(candidate);
                    compact = true;
                }
            }
        }

        const sequence = ++this.#sequence;
        this.#tombstones.delete(uri);
        const normalized = { ...entry, include_self: entry.include_self ?? true };
        this.#tombstones.set(uri, { entry: normalized, sequence });
        await this._enqueue(() => this._append({ op: 'put', uri, entry: normalized, sequence }));
        if (await this._enforce_limit()) compact = true;
        if (compact || this.#journal_records > Math.max(1, this.#max_tombstones * 2)) await this.compact();
        else this.schedule_snapshot();
        return normalized;
    }

    async _enforce_limit() {
        let evicted = false;
        while (this.#tombstones.size > this.#max_tombstones) {
            const oldest = this.#tombstones.keys().next().value;
            this.#tombstones.delete(oldest);
            this.#log('TOMBSTONE_EVICTED', oldest);
            evicted = true;
        }
        return evicted;
    }

    /** @returns {void} Schedules one coalesced snapshot without keeping the process alive. */
    schedule_snapshot() {
        if (this.#closed || this.#snapshot_timer) return;
        this.#snapshot_timer = setTimeout(() => {
            this.#snapshot_timer = undefined;
            this.compact().catch((error) => this.#log('STATE_ERROR', error.message));
        }, 100);
        this.#snapshot_timer.unref?.();
    }

    /**
     * Atomically snapshots current state and truncates the replay journal.
     *
     * @returns {Promise<void>}
     */
    compact() {
        return this._enqueue(async () => {
            const temporary = Path.join(this.#state_path, `.manifest-${randomUUID()}.tmp`);
            const snapshot = {
                version: STATE_VERSION,
                sequence: this.#sequence,
                active: Object.fromEntries(this.#active),
                tombstones: [...this.#tombstones].map(([uri, value]) => ({
                    op: 'put',
                    uri,
                    entry: value.entry,
                    sequence: value.sequence,
                })),
            };
            await FileSystem.writeFile(temporary, JSON.stringify(snapshot), 'utf8');
            await FileSystem.rename(temporary, this.#manifest_path);
            await FileSystem.writeFile(this.#ledger_path, '', 'utf8');
            this.#journal_records = 0;
        });
    }

    /** @returns {Promise<void>} Flushes a final snapshot and closes the store. Idempotent. */
    async close() {
        if (this.#closed) return;
        if (this.#snapshot_timer) clearTimeout(this.#snapshot_timer);
        this.#snapshot_timer = undefined;
        try {
            await FileSystem.access(this.#state_path);
            await this.compact();
        } catch (error) {
            if (error.code !== 'ENOENT') throw error;
        }
        this.#closed = true;
    }

    /** @returns {Map<string, import('../../../index.js').Entry>} Live active-entry map. */
    get active() {
        return this.#active;
    }

    /** @returns {Map<string, import('../../../index.js').TombstoneEntry>} Copy of retained tombstones in eviction order. */
    get tombstones() {
        return new Map([...this.#tombstones].map(([uri, value]) => [uri, value.entry]));
    }

    /** @returns {string} Absolute state directory. */
    get path() {
        return this.#state_path;
    }

    /** @returns {string} Temporary-file directory used for atomic content writes. */
    get tmp_path() {
        return Path.join(this.#state_path, 'tmp');
    }
}
