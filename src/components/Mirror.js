import Crypto, { randomUUID } from 'node:crypto';
import EventEmitter from 'node:events';
import FileSystem from 'node:fs/promises';
import Path from 'node:path';
import Websocket from 'ws';
import DirectoryMap from './directory/DirectoryMap.js';
import DirectoryManager from './directory/DirectoryManager.js';
import {
    ACTOR_HEADER,
    AUTH_HEADER,
    BASE_REVISION_HEADER,
    EPOCH_HEADER,
    HASH_HEADER,
    MODIFIED_HEADER,
    PROTOCOL_HEADER,
    PROTOCOL_PATH,
    PROTOCOL_VERSION,
    REVISION_HEADER,
} from '../constants.js';
import { KeyedQueue, TaskQueue } from '../utils/queues.js';
import {
    abort_error,
    async_wait,
    canonicalize_uri,
    compare_entries,
    entries_equivalent,
    merge_options,
    safe_json_parse,
    validate_entry,
} from '../utils/operators.js';

const DEFAULT_OPTIONS = {
    path: '',
    hostname: '',
    port: 8080,
    ssl: false,
    auth: '',
    retry: { every: 1_000, backoff: true },
    queue: {
        max_concurrent: 100,
        max_queued: Infinity,
        timeout: Infinity,
        throttle: { rate: Infinity, interval: Infinity },
    },
    state: {},
    hashing: {},
    limits: { max_file_size: 100 * 1024 * 1024 },
    watcher: {},
    filters: undefined,
};

const websocket_pool = new Map();

function pool_identity(options) {
    const scheme = options.ssl ? 'wss' : 'ws';
    const auth = Crypto.createHash('sha256').update(options.auth).digest('hex');
    return `${scheme}://${options.hostname}:${options.port}/${auth}`;
}

/**
 * Multiplexes repository event subscriptions over one socket per endpoint and
 * authentication identity. This avoids one WebSocket allocation per Mirror.
 */
class PooledWebsocket {
    #attempt = 0;
    #closed = false;
    #ever_opened = false;
    #identity;
    #options;
    #pending_subscriptions = new Map();
    #reconnect_timer;
    #socket;
    #subscriptions = new Map();

    constructor(identity, options) {
        this.#identity = identity;
        this.#options = options;
    }

    add(mirror) {
        let mirrors = this.#subscriptions.get(mirror.host);
        if (!mirrors) this.#subscriptions.set(mirror.host, (mirrors = new Set()));
        mirrors.add(mirror);
        if (this.#socket?.readyState === Websocket.OPEN)
            this._subscribe(mirror.host, [mirror], false);
        else this._connect();
    }

    remove(mirror) {
        const mirrors = this.#subscriptions.get(mirror.host);
        mirrors?.delete(mirror);
        if (mirrors?.size === 0) this.#subscriptions.delete(mirror.host);
        if (this.#subscriptions.size) return;
        this.#closed = true;
        if (this.#reconnect_timer) clearTimeout(this.#reconnect_timer);
        this.#reconnect_timer = undefined;
        this.#socket?.close(1000, 'No subscriptions');
        websocket_pool.delete(this.#identity);
    }

    _subscribe(host, mirrors, reconnect) {
        // Do not declare a mirror connected until Server acknowledges SUBSCRIBE.
        // The subsequent hard sync closes the manifest-to-subscription race.
        let pending = this.#pending_subscriptions.get(host);
        if (!pending) this.#pending_subscriptions.set(host, (pending = new Map()));
        for (const mirror of mirrors) pending.set(mirror, reconnect);
        this.#socket.send(JSON.stringify({ command: 'SUBSCRIBE', host }));
    }

    _all_mirrors() {
        return [...this.#subscriptions.values()].flatMap((mirrors) => [...mirrors]);
    }

    _connect() {
        if (
            this.#closed ||
            this.#socket?.readyState === Websocket.OPEN ||
            this.#socket?.readyState === Websocket.CONNECTING
        ) return;
        const { auth, hostname, port, ssl } = this.#options;
        const socket = new Websocket(`${ssl ? 'wss' : 'ws'}://${hostname}:${port}${PROTOCOL_PATH}/events`, {
            headers: {
                [AUTH_HEADER]: auth,
                [PROTOCOL_HEADER]: String(PROTOCOL_VERSION),
                [ACTOR_HEADER]: `pool-${this.#identity.slice(-16)}`,
            },
        });
        this.#socket = socket;
        socket.on('open', () => {
            const reconnect = this.#ever_opened;
            this.#ever_opened = true;
            this.#attempt = 0;
            for (const [host, mirrors] of this.#subscriptions)
                this._subscribe(host, mirrors, reconnect);
        });
        socket.on('message', (raw) => {
            const message = safe_json_parse(String(raw));
            if (message?.command === 'SUBSCRIBED' && message.protocol === PROTOCOL_VERSION) {
                const pending = this.#pending_subscriptions.get(message.host);
                this.#pending_subscriptions.delete(message.host);
                for (const [mirror, reconnect] of pending || []) {
                    if (this.#subscriptions.get(message.host)?.has(mirror))
                        mirror._on_socket_ready(reconnect);
                }
                return;
            }
            if (message?.command !== 'MUTATION' || message.protocol !== PROTOCOL_VERSION) return;
            for (const mirror of this.#subscriptions.get(message.host) || [])
                mirror._receive_mutation(message);
        });
        socket.on('error', (error) => {
            for (const mirror of this._all_mirrors()) mirror._emit_error(error);
        });
        socket.on('close', () => {
            if (this.#socket === socket) this.#socket = undefined;
            this.#pending_subscriptions.clear();
            if (this.#closed || !this.#subscriptions.size) return;
            const retry = this.#options.retry;
            const base = retry.backoff ? retry.every * 2 ** this.#attempt++ : retry.every;
            const delay = Math.min(30_000, Math.max(0, base)) * (0.8 + Math.random() * 0.4);
            this.#reconnect_timer = setTimeout(() => {
                this.#reconnect_timer = undefined;
                this._connect();
            }, delay);
            this.#reconnect_timer.unref?.();
        });
    }
}

function index_tombstones(records) {
    const directories = new Map();
    const exact = new Map();
    for (const record of records || []) {
        if (canonicalize_uri(record.uri) !== record.uri) throw new TypeError('Tombstone URI must be canonical.');
        validate_entry(record.entry);
        if (record.entry.type !== 'tombstone') throw new TypeError('Expected a tombstone entry.');
        if (!Number.isSafeInteger(record.revision) || record.revision < 0)
            throw new TypeError('Tombstone revision must be a non-negative safe integer.');
        const current = exact.get(record.uri);
        if (
            record.entry.include_self !== false &&
            (!current || record.revision > current.revision || (
                record.revision === current.revision &&
                record.entry.deleted_at > current.entry.deleted_at
            ))
        ) exact.set(record.uri, record);
        if (record.entry.target === 'directory') {
            const directory = directories.get(record.uri);
            if (!directory || record.revision > directory.revision || (
                record.revision === directory.revision &&
                record.entry.deleted_at > directory.entry.deleted_at
            ))
                directories.set(record.uri, record);
        }
    }
    return { directories, exact };
}

function effective_tombstone(index, uri) {
    let selected = index.exact.get(uri);
    let cursor = Path.posix.dirname(uri);
    while (cursor && cursor !== '/') {
        const record = index.directories.get(cursor);
        if (record && (!selected || record.revision > selected.revision || (
            record.revision === selected.revision &&
            record.entry.deleted_at > selected.entry.deleted_at
        ))) selected = record;
        const parent = Path.posix.dirname(cursor);
        if (parent === cursor) break;
        cursor = parent;
    }
    return selected;
}

function action_priority(entry) {
    if (entry.type === 'tombstone') return 0;
    if (entry.type === 'directory') return 1;
    return 2;
}

function uri_depth(uri) {
    let depth = 0;
    for (let index = 0; index < uri.length; index++)
        if (uri.charCodeAt(index) === 47) depth++;
    return depth;
}

function yield_event_loop() {
    return new Promise((resolve) => setImmediate(resolve));
}

/**
 * Bidirectional repository node backed by a Server authority.
 *
 * Local watcher mutations are pushed to the authority. Accepted server entries
 * are pulled over HTTP and applied atomically, while WebSocket messages provide
 * low-latency invalidation. Full manifests are reconciled at startup and after
 * each confirmed socket connection so WebSocket delivery is never the sole
 * source of truth.
 *
 * @extends EventEmitter
 */
export default class Mirror extends EventEmitter {
    #abort_controller = new AbortController();
    #actor = randomUUID();
    #clock_offset = 0;
    #destroy_promise;
    #destroyed = false;
    #epoch;
    #event_chain = Promise.resolve();
    #host;
    #initialized = false;
    #initial_remote_promise;
    #manager;
    #map;
    #options;
    #pending_mutations = [];
    #pool;
    #ready_promise;
    #reconcile_again = false;
    #reconcile_promise;
    #transfer_queue;
    #versions = new Map();
    #work = new KeyedQueue();
    #socket_ready_promise;
    #socket_ready_resolve;

    /**
     * Creates a mirror and begins its initial authority reconciliation.
     *
     * @param {string} host Name passed to Server.host().
     * @param {import('../../index.js').MirrorOptions} [options={}] Local path, endpoint, authentication, and queue options.
     */
    constructor(host, options = {}) {
        super();
        if (typeof host !== 'string' || !host)
            throw new TypeError('new DirectorySync.Mirror(host, options) -> host must be a non-empty string.');
        if (!options || typeof options !== 'object')
            throw new TypeError('new DirectorySync.Mirror(host, options) -> options must be an object.');
        this.#host = host;
        this.#options = merge_options(DEFAULT_OPTIONS, options);
        if (typeof this.#options.path !== 'string' || !this.#options.path)
            throw new TypeError('Mirror path must be a non-empty string.');
        if (typeof this.#options.hostname !== 'string' || !this.#options.hostname)
            throw new TypeError('Mirror hostname must be a non-empty string.');
        if (!Number.isInteger(this.#options.port) || this.#options.port < 1 || this.#options.port > 65_535)
            throw new TypeError('Mirror port must be an integer between 1 and 65535.');
        if (!Number.isFinite(this.#options.retry.every) || this.#options.retry.every < 0)
            throw new TypeError('retry.every must be a non-negative number.');
        this.#transfer_queue = new TaskQueue({
            concurrency: this.#options.queue.max_concurrent,
            maximum: this.#options.queue.max_queued,
            timeout: this.#options.queue.timeout,
            throttle: this.#options.queue.throttle,
        });
        this.#socket_ready_promise = new Promise((resolve) => {
            this.#socket_ready_resolve = resolve;
        });
        this.#ready_promise = this._initialize();
        this.#ready_promise.catch((error) => this._emit_error(error));
    }

    /** @returns {string} Remote repository name. */
    get host() { return this.#host; }

    /** @protected @param {string} code Log category. @param {string} message Human-readable detail. @returns {void} */
    _log(code, message) {
        this.emit('log', code, message);
    }

    /** @protected @param {Error} error Transfer or watcher error. @returns {void} */
    _emit_error(error) {
        if (this.#destroyed && error?.name === 'AbortError') return;
        this.emit('error', error);
    }

    async _initialize() {
        const root = Path.resolve(this.#options.path);
        await FileSystem.mkdir(root, { recursive: true });
        const map_options = merge_options({}, {
            path: root,
            state: { ...this.#options.state, max_tombstones: 0 },
            hashing: this.#options.hashing,
            limits: this.#options.limits,
            watcher: this.#options.watcher,
        });
        if (this.#options.filters !== undefined) map_options.filters = this.#options.filters;
        this.#map = new DirectoryMap(map_options);
        this.#manager = new DirectoryManager(this.#map, true);
        this.#map.on('error', (error) => this._emit_error(error));
        this.#map.on('log', (code, message) => this._log(code, message));
        this.#map.on('file_size_limit', (uri, record) =>
            this._log('SIZE_LIMIT_REACHED', `${uri} - SIZE_${record.stats.size}_BYTES`)
        );

        const identity = pool_identity(this.#options);
        this.#pool = websocket_pool.get(identity);
        if (!this.#pool) {
            this.#pool = new PooledWebsocket(identity, this.#options);
            websocket_pool.set(identity, this.#pool);
        }
        this.#pool.add(this);
        await Promise.all([this.#map.ready(), this.#socket_ready_promise]);
        const remote = await this.#initial_remote_promise;
        await this._perform_hard_sync(remote);
        await this.#map.settled();
        this.#map.on('mutation', (uri, entry) => this._handle_local_mutation(uri, entry));
        this.#initialized = true;
        for (const message of this.#pending_mutations.splice(0)) this._receive_mutation(message);
        return this;
    }

    async _fetch_manifest() {
        const started = Date.now();
        const response = await this._request('GET', `${PROTOCOL_PATH}/manifest`);
        const received = Date.now();
        const payload = await response.json();
        if (
            payload.protocol !== PROTOCOL_VERSION ||
            typeof payload.epoch !== 'string' || !payload.epoch ||
            !payload.manifest ||
            !Array.isArray(payload.tombstones) ||
            !payload.versions || typeof payload.versions !== 'object' ||
            !Number.isSafeInteger(payload.server_time)
        ) throw new Error(`Remote does not support DirectorySync protocol v${PROTOCOL_VERSION}.`);
        for (const uri of Object.keys(payload.manifest)) {
            const revision = payload.versions[uri];
            if (!Number.isSafeInteger(revision) || revision < 0)
                throw new TypeError(`Manifest revision for ${uri} must be a non-negative safe integer.`);
        }
        if (this.#epoch && this.#epoch !== payload.epoch) this.#versions.clear();
        this.#epoch = payload.epoch;
        const offset = Math.round((started + received) / 2 - payload.server_time);
        const uncertainty = Math.ceil((received - started) / 2) + 1;
        // A one-shot midpoint estimate cannot distinguish a small clock offset
        // from request latency. Treat that uncertainty band as zero so jitter
        // cannot turn an exact mtime tie into a newer Mirror mutation.
        this.#clock_offset = Math.abs(offset) <= uncertainty ? 0 : offset;
        return payload;
    }

    _shift_entry(entry, offset) {
        if (!entry) return undefined;
        const shifted = { ...entry };
        const key = entry.type === 'tombstone' ? 'deleted_at' : 'modified_at';
        shifted[key] = Math.max(0, Math.min(Number.MAX_SAFE_INTEGER, Math.round(entry[key] + offset)));
        return shifted;
    }

    _normalize_local(entry) {
        return this._shift_entry(entry, -this.#clock_offset);
    }

    _localize_remote(entry) {
        return this._shift_entry(entry, this.#clock_offset);
    }

    _url(endpoint, uri) {
        const url = new URL(
            `http${this.#options.ssl ? 's' : ''}://${this.#options.hostname}:${this.#options.port}${endpoint}`
        );
        url.searchParams.set('host', this.#host);
        if (uri) url.searchParams.set('uri', uri);
        return url;
    }

    async _request(method, endpoint, options = {}) {
        let attempt = 0;
        while (!this.#destroyed) {
            let body;
            try {
                body = options.body_factory ? await options.body_factory() : options.body;
                const headers = {
                    [AUTH_HEADER]: this.#options.auth,
                    [PROTOCOL_HEADER]: String(PROTOCOL_VERSION),
                    [ACTOR_HEADER]: this.#actor,
                    ...options.headers,
                };
                if (options.json !== undefined) {
                    body = JSON.stringify(options.json);
                    headers['content-type'] = 'application/json';
                }
                const init = { method, headers, body, signal: this.#abort_controller.signal };
                if (body && typeof body.pipe === 'function') init.duplex = 'half';
                const response = await fetch(this._url(endpoint, options.uri), init);
                if ([408, 425, 429].includes(response.status) || response.status >= 500) {
                    await response.body?.cancel().catch(() => undefined);
                    const error = new Error(`HTTP ${response.status}`);
                    error.retryable = true;
                    throw error;
                }
                if ([401, 403, 413, 422, 426].includes(response.status)) {
                    const payload = await response.json().catch(() => ({}));
                    const error = new Error(payload.message || `HTTP ${response.status}`);
                    error.code = payload.code || `HTTP_${response.status}`;
                    error.status = response.status;
                    throw error;
                }
                return response;
            } catch (error) {
                body?.destroy?.();
                if (this.#destroyed || this.#abort_controller.signal.aborted) throw abort_error();
                if (!error.retryable && error.status) throw error;
                if (['ENOENT', 'EACCES', 'EPERM'].includes(error.code)) throw error;
                const retry = this.#options.retry;
                const base = retry.backoff ? retry.every * 2 ** attempt++ : retry.every;
                const delay = Math.min(30_000, Math.max(0, base)) * (0.8 + Math.random() * 0.4);
                this._log('HTTP', `ERROR - ${method} ${endpoint} - ${error.message} - RETRY[${Math.round(delay)}ms]`);
                await async_wait(delay, this.#abort_controller.signal);
            }
        }
        throw abort_error();
    }

    _schedule(uri, handler) {
        return this.#work.run(uri, () =>
            this.#transfer_queue.run(handler, this.#abort_controller.signal)
        );
    }

    _revision_headers(uri, revision = this.#versions.get(uri)) {
        const headers = { [EPOCH_HEADER]: this.#epoch };
        if (revision !== undefined) headers[BASE_REVISION_HEADER] = String(revision);
        return headers;
    }

    _adopt_revision(uri, entry, revision, epoch = this.#epoch) {
        if (epoch !== this.#epoch || !Number.isSafeInteger(revision) || revision < 0) return false;
        const current = this.#versions.get(uri);
        if (current !== undefined && current > revision) return false;
        this.#versions.set(uri, revision);
        if (entry?.type === 'tombstone' && entry.target === 'directory')
            for (const candidate of this.#versions.keys())
                if (candidate.startsWith(`${uri}/`)) this.#versions.set(candidate, revision);
        return true;
    }

    async _apply_remote(uri, entry, revision, epoch = this.#epoch) {
        validate_entry(entry);
        if (epoch !== this.#epoch) {
            this._perform_hard_sync().catch((error) => this._emit_error(error));
            return;
        }
        if (!Number.isSafeInteger(revision) || revision < 0)
            throw new TypeError('Authority revision must be a non-negative safe integer.');
        const known = this.#versions.get(uri);
        if (known !== undefined && known > revision) return;
        const local = this.#map.canonical_entry(uri);
        const localized = this._localize_remote(entry);
        if (
            entry.type === 'file' &&
            local?.type === 'file' &&
            local.size === entry.size &&
            local.sha256 === entry.sha256
        ) {
            if (!entries_equivalent(local, localized)) await this.#manager.apply_metadata(uri, localized);
            this._adopt_revision(uri, entry, revision, epoch);
            return;
        }
        if (entry.type === 'directory') {
            await this.#manager.apply_directory(uri, localized);
            this._adopt_revision(uri, entry, revision, epoch);
            return;
        }
        if (entry.type === 'tombstone') {
            await this.#manager.apply_tombstone(uri, localized);
            this._adopt_revision(uri, entry, revision, epoch);
            return;
        }

        const start = Date.now();
        this._log('DOWNLOAD', `${uri} - FILE - START`);
        const response = await this._request('GET', `${PROTOCOL_PATH}/content`, { uri });
        if (response.status === 404) return;
        const actual_epoch = response.headers.get(EPOCH_HEADER);
        const actual_revision = Number(response.headers.get(REVISION_HEADER));
        const actual = {
            type: 'file',
            modified_at: Number(response.headers.get(MODIFIED_HEADER)),
            size: Number(response.headers.get('content-length')),
            sha256: response.headers.get(HASH_HEADER),
        };
        validate_entry(actual);
        if (
            actual_epoch !== this.#epoch ||
            !Number.isSafeInteger(actual_revision) || actual_revision < revision
        ) {
            await response.body?.cancel();
            this._perform_hard_sync().catch((error) => this._emit_error(error));
            return;
        }
        const actual_local = this._localize_remote(actual);
        await this.#manager.apply_file(uri, response.body, actual_local, {
            validate_before_commit: () => {
                const current = this.#map.canonical_entry(uri);
                return (!current && !local) || entries_equivalent(current, local);
            },
        });
        this._adopt_revision(uri, actual, actual_revision, actual_epoch);
        this._log('DOWNLOAD', `${uri} - FILE - COMPLETE - ${Date.now() - start}ms`);
    }

    async _push_local(uri, entry, remote) {
        if (!entry) return;
        validate_entry(entry);
        // Queue entries are immutable snapshots. If the watcher has already seen
        // a newer local state, discard this stale transfer instead of uploading
        // content that no longer corresponds to the path on disk.
        if (entry.type !== 'tombstone' && !entries_equivalent(this.#map.canonical_entry(uri), entry)) return;
        if (entry.type === 'file') {
            try {
                if (!(await FileSystem.lstat(this.#map.resolve(uri))).isFile()) return;
            } catch (error) {
                if (error.code === 'ENOENT') return;
                throw error;
            }
        }
        const wire_entry = this._normalize_local(entry);
        const remote_entry = remote?.entry;
        const headers = this._revision_headers(uri, remote?.revision);
        let response;
        if (entry.type === 'directory') {
            response = await this._request('PUT', `${PROTOCOL_PATH}/directory`, {
                uri,
                json: wire_entry,
                headers,
            });
        } else if (entry.type === 'tombstone') {
            response = await this._request('DELETE', `${PROTOCOL_PATH}/entry`, {
                uri,
                json: wire_entry,
                headers,
            });
        } else if (
            remote_entry?.type === 'file' &&
            remote_entry.size === entry.size &&
            remote_entry.sha256 === entry.sha256
        ) {
            response = await this._request('PATCH', `${PROTOCOL_PATH}/content`, {
                uri,
                json: wire_entry,
                headers,
            });
        } else {
            const start = Date.now();
            this._log('UPLOAD', `${uri} - FILE - START`);
            response = await this._request('PUT', `${PROTOCOL_PATH}/content`, {
                uri,
                headers: {
                    ...headers,
                    [MODIFIED_HEADER]: String(wire_entry.modified_at),
                    [HASH_HEADER]: entry.sha256,
                    'content-length': String(entry.size),
                },
                body_factory: entry.size ? () => this.#manager.read(uri, true) : undefined,
            });
            this._log('UPLOAD', `${uri} - FILE - COMPLETE - ${Date.now() - start}ms`);
        }
        const payload = await response.json().catch(() => ({}));
        if (
            payload.epoch !== this.#epoch ||
            !Number.isSafeInteger(payload.revision) || payload.revision < 0
        ) {
            this._perform_hard_sync().catch((error) => this._emit_error(error));
            return;
        }
        if (response.status === 409) {
            if (payload.entry)
                await this._apply_remote(uri, payload.entry, payload.revision, payload.epoch);
            return;
        }
        if (payload.entry && !entries_equivalent(payload.entry, wire_entry))
            await this._apply_remote(uri, payload.entry, payload.revision, payload.epoch);
        else this._adopt_revision(uri, payload.entry, payload.revision, payload.epoch);
    }

    async _plan(remote, local) {
        const actions = new Map();
        const tombstones = index_tombstones(remote.tombstones);
        const normalized_local = Object.create(null);
        const effective_tombstones = new Map();
        const local_uris = Object.keys(local);
        let iterations = 0;
        for (const uri of local_uris) {
            normalized_local[uri] = this._normalize_local(local[uri]);
            if (++iterations % 2_048 === 0) await yield_event_loop();
        }

        const uris = new Set(local_uris);
        for (const uri of Object.keys(remote.manifest)) uris.add(uri);
        for (const record of remote.tombstones) uris.add(record.uri);
        for (const uri of uris) {
            if (++iterations % 2_048 === 0) await yield_event_loop();
            const active = remote.manifest[uri];
            const active_record = active ? {
                uri,
                entry: active,
                revision: remote.versions[uri],
            } : undefined;
            let tombstone = effective_tombstones.get(uri);
            if (!effective_tombstones.has(uri)) {
                tombstone = effective_tombstone(tombstones, uri);
                effective_tombstones.set(uri, tombstone);
            }
            let server = active_record;
            if (
                tombstone &&
                !(active_record && tombstone.entry.target === 'directory' && tombstone.entry.include_self === false) &&
                (!server || tombstone.revision > server.revision || (
                    tombstone.revision === server.revision &&
                    compare_entries(server.entry, tombstone.entry) === 1
                ))
            ) server = tombstone;
            const mirror = normalized_local[uri];
            if (
                server?.entry.type === 'file' &&
                mirror?.type === 'file' &&
                server.entry.size === mirror.size &&
                server.entry.sha256 === mirror.sha256
            ) {
                if (!entries_equivalent(server.entry, mirror)) {
                    actions.set(`pull:file:${uri}`, {
                        direction: 'pull',
                        uri,
                        record: server,
                    });
                } else actions.set(`adopt:${uri}`, { direction: 'adopt', uri, record: server });
                continue;
            }
            if (server && entries_equivalent(server.entry, mirror)) {
                actions.set(`adopt:${uri}`, { direction: 'adopt', uri, record: server });
                continue;
            }
            const known = this.#versions.get(uri);
            if (server?.uri === uri && known !== undefined && known === server.revision && mirror) {
                actions.set(`push:${mirror.type}:${uri}`, {
                    direction: 'push',
                    uri,
                    entry: local[uri],
                    other: server,
                });
                continue;
            }
            const comparison = compare_entries(server?.entry, mirror);
            if (comparison < 0 && server) {
                actions.set(`pull:${server.entry.type}:${server.uri}`, {
                    direction: 'pull',
                    uri: server.uri,
                    record: server,
                });
            } else if (comparison > 0 && mirror) {
                actions.set(`push:${mirror.type}:${uri}`, {
                    direction: 'push',
                    uri,
                    entry: local[uri],
                    other: server,
                });
            }
        }
        const output = [...actions.values()];
        for (const action of output) {
            action.depth = uri_depth(action.uri);
            action.type = action.entry?.type || action.record.entry.type;
        }
        return output.sort((left, right) => {
            const priority = action_priority({ type: left.type }) - action_priority({ type: right.type });
            if (priority) return priority;
            const depth = left.depth - right.depth;
            if (left.type === 'tombstone' && depth) return -depth;
            return depth || left.uri.localeCompare(right.uri);
        });
    }

    async _reconcile_once(remote_state) {
        if (!remote_state) remote_state = await this._fetch_manifest();
        const remote = {
            manifest: remote_state.manifest,
            tombstones: remote_state.tombstones,
            versions: remote_state.versions,
        };
        const actions = await this._plan(remote, this.#map.manifest);
        const perform = (action) => this._schedule(action.uri, async () => {
            try {
                if (action.direction === 'adopt') {
                    this._adopt_revision(
                        action.uri,
                        action.record.entry,
                        action.record.revision,
                        remote_state.epoch
                    );
                } else if (action.direction === 'pull') {
                    await this._apply_remote(
                        action.uri,
                        action.record.entry,
                        action.record.revision,
                        remote_state.epoch
                    );
                } else await this._push_local(action.uri, action.entry, action.other);
                return true;
            } catch (error) {
                if (error.code !== 'FILTERED') throw error;
                this._log('FILTERED', `${action.uri} - rejected by authority filter`);
                return false;
            }
        });
        // Tombstones go first to remove stale type conflicts, directories create
        // parents for files, and a final deepest-first directory metadata pass
        // restores mtimes changed while child files were written.
        const directories = [];
        const files = [];
        for (const action of actions)
            if (action.direction === 'adopt') await perform(action);
            else if (action.type === 'tombstone') await perform(action);
            else if (action.type === 'directory') directories.push(action);
            else files.push(action);
        const applied_directories = [];
        for (const action of directories)
            if (await perform(action)) applied_directories.push(action);
        const transfers = [];
        for (const action of files) transfers.push(perform(action));
        await Promise.all(transfers);
        applied_directories.sort((left, right) => right.depth - left.depth);
        for (const action of applied_directories) await perform(action);
    }

    /**
     * Reconciles a complete authority manifest, coalescing overlapping requests.
     *
     * @protected
     * @param {object} [state] Optional already-fetched authority state.
     * @returns {Promise<void>}
     */
    _perform_hard_sync(state) {
        if (this.#reconcile_promise) {
            this.#reconcile_again = true;
            return this.#reconcile_promise;
        }
        this.#reconcile_promise = (async () => {
            const start = Date.now();
            this._log('HARD_SYNC', 'START');
            await this._reconcile_once(state);
            while (this.#reconcile_again && !this.#destroyed) {
                this.#reconcile_again = false;
                await this._reconcile_once();
            }
            this._log('HARD_SYNC', `COMPLETE - ${Date.now() - start}ms`);
        })().finally(() => {
            this.#reconcile_promise = undefined;
        });
        return this.#reconcile_promise;
    }

    _handle_local_mutation(uri, entry) {
        this._schedule(uri, () => this._push_local(uri, entry))
            .catch((error) => {
                if (error.code === 'FILTERED')
                    return this._log('FILTERED', `${uri} - rejected by authority filter`);
                if (!this.#destroyed) this._emit_error(error);
            });
    }

    /** @protected @param {object} message Protocol mutation event. @returns {void} */
    _receive_mutation(message) {
        if (message.actor === this.#actor || this.#destroyed) return;
        if (!this.#initialized) {
            this.#pending_mutations.push(message);
            return;
        }
        this.#event_chain = this.#event_chain
            .catch(() => undefined)
            .then(() => this._schedule(message.uri, () =>
                this._apply_remote(message.uri, message.entry, message.revision, message.epoch)
            ));
        this.#event_chain.catch((error) => {
            if (!this.#destroyed) this._emit_error(error);
        });
    }

    /** @protected @param {boolean} reconnect Whether this socket was previously connected. @returns {void} */
    _on_socket_ready(reconnect) {
        this._log('WEBSOCKET', `${reconnect ? 'RECONNECTED' : 'CONNECTED'} - ${this.#options.hostname}:${this.#options.port}`);
        if (!this.#initialized) {
            this.#initial_remote_promise ||= this._fetch_manifest();
            this.#socket_ready_resolve();
            return;
        }
        this._perform_hard_sync().catch((error) => this._emit_error(error));
    }

    /**
     * Waits for the local map and initial two-pass manifest reconciliation.
     *
     * @returns {Promise<Mirror>} This mirror instance.
     */
    ready() {
        return this.#ready_promise;
    }

    /**
     * Stops retries and transfers, leaves the shared event socket, and closes the watcher. Idempotent.
     *
     * @returns {Promise<void>}
     */
    destroy() {
        if (this.#destroy_promise) return this.#destroy_promise;
        this.#destroyed = true;
        this.#abort_controller.abort();
        this.#transfer_queue.close();
        this.#work.close();
        this.#pool?.remove(this);
        this.#destroy_promise = (async () => {
            await this.#ready_promise.catch(() => undefined);
            await this.#event_chain.catch(() => undefined);
            await this.#work.idle();
            await this.#map?.destroy();
            this.removeAllListeners();
        })();
        return this.#destroy_promise;
    }

    /** @returns {boolean} Whether destroy() has started. */
    get destroyed() { return this.#destroyed; }

    /** @returns {import('../../index.js').MirrorOptions} Effective mirror options. */
    get options() { return this.#options; }

    /** @returns {import('../../index.js').DirectoryMap} Live local filesystem map after ready(). */
    get map() { return this.#map; }
}
