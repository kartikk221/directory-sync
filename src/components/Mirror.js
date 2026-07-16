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
    HASH_HEADER,
    MODIFIED_HEADER,
    PROTOCOL_HEADER,
    PROTOCOL_VERSION,
} from '../constants.js';
import { KeyedQueue, TaskQueue } from '../utils/queues.js';
import {
    abort_error,
    async_wait,
    compare_entries,
    entries_equivalent,
    entry_timestamp,
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
        max_concurrent: 10,
        max_queued: Infinity,
        timeout: Infinity,
        throttle: { rate: Infinity, interval: Infinity },
    },
    state: {},
    hashing: {},
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
        const socket = new Websocket(`${ssl ? 'wss' : 'ws'}://${hostname}:${port}/v3/events`, {
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

function effective_ref(manifest, uri) {
    // A directory tombstone is also the effective entry for older descendants.
    // Returning its own URI deduplicates a whole deleted subtree into one action.
    let selected = manifest[uri] ? { uri, entry: manifest[uri] } : undefined;
    let parent = Path.posix.dirname(uri);
    while (parent && parent !== '/') {
        const candidate = manifest[parent];
        if (
            candidate?.type === 'tombstone' &&
            candidate.target === 'directory' &&
            (!selected || candidate.deleted_at >= entry_timestamp(selected.entry))
        ) selected = { uri: parent, entry: candidate };
        const next = Path.posix.dirname(parent);
        if (next === parent) break;
        parent = next;
    }
    return selected;
}

function action_priority(entry) {
    if (entry.type === 'tombstone') return 0;
    if (entry.type === 'directory') return 1;
    return 2;
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
    #destroy_promise;
    #destroyed = false;
    #event_chain = Promise.resolve();
    #host;
    #manager;
    #map;
    #options;
    #pool;
    #ready_promise;
    #reconcile_again = false;
    #reconcile_promise;
    #transfer_queue;
    #work = new KeyedQueue();

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
        const response = await this._request('GET', '/v3/manifest');
        const remote = await response.json();
        if (remote.protocol !== PROTOCOL_VERSION || !remote.manifest)
            throw new Error(`Remote does not support DirectorySync protocol v${PROTOCOL_VERSION}.`);

        const map_options = merge_options(remote.options || {}, {
            path: root,
            state: this.#options.state,
            hashing: this.#options.hashing,
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
        await this.#map.ready();
        await this._perform_hard_sync(remote.manifest, true);
        this.#map.on('mutation', (uri, entry) => this._handle_local_mutation(uri, entry));

        const identity = pool_identity(this.#options);
        this.#pool = websocket_pool.get(identity);
        if (!this.#pool) {
            this.#pool = new PooledWebsocket(identity, this.#options);
            websocket_pool.set(identity, this.#pool);
        }
        this.#pool.add(this);
        return this;
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

    async _apply_remote(uri, entry) {
        validate_entry(entry);
        const local = this.#map.canonical_entry(uri);
        if (compare_entries(entry, local) === 1) return this._push_local(uri, local, entry);
        if (entry.type === 'directory') return this.#manager.apply_directory(uri, entry);
        if (entry.type === 'tombstone') return this.#manager.apply_tombstone(uri, entry);
        if (
            local?.type === 'file' &&
            local.size === entry.size &&
            local.sha256 === entry.sha256
        ) return this.#manager.apply_metadata(uri, entry);

        const start = Date.now();
        this._log('DOWNLOAD', `${uri} - FILE - START`);
        const response = await this._request('GET', '/v3/content', { uri });
        if (response.status === 404) return;
        const actual = {
            type: 'file',
            modified_at: Number(response.headers.get(MODIFIED_HEADER)),
            size: Number(response.headers.get('content-length')),
            sha256: response.headers.get(HASH_HEADER),
        };
        validate_entry(actual);
        if (compare_entries(actual, this.#map.canonical_entry(uri)) === 1) {
            await response.body?.cancel();
            return this._push_local(uri, this.#map.canonical_entry(uri), actual);
        }
        await this.#manager.apply_file(uri, response.body, actual, {
            validate_before_commit: () =>
                compare_entries(actual, this.#map.canonical_entry(uri)) <= 0,
        });
        this._log('DOWNLOAD', `${uri} - FILE - COMPLETE - ${Date.now() - start}ms`);
    }

    async _push_local(uri, entry, remote) {
        if (!entry) return;
        validate_entry(entry);
        // Queue entries are immutable snapshots. If the watcher has already seen
        // a newer local state, discard this stale transfer instead of uploading
        // content that no longer corresponds to the path on disk.
        if (!entries_equivalent(this.#map.canonical_entry(uri), entry)) return;
        if (entry.type === 'file') {
            try {
                if (!(await FileSystem.lstat(this.#map.resolve(uri))).isFile()) return;
            } catch (error) {
                if (error.code === 'ENOENT') return;
                throw error;
            }
        }
        let response;
        if (entry.type === 'directory') {
            response = await this._request('PUT', '/v3/directory', { uri, json: entry });
        } else if (entry.type === 'tombstone') {
            response = await this._request('DELETE', '/v3/entry', { uri, json: entry });
        } else if (
            remote?.type === 'file' &&
            remote.size === entry.size &&
            remote.sha256 === entry.sha256
        ) {
            response = await this._request('PATCH', '/v3/content', { uri, json: entry });
        } else {
            const start = Date.now();
            this._log('UPLOAD', `${uri} - FILE - START`);
            response = await this._request('PUT', '/v3/content', {
                uri,
                headers: {
                    [MODIFIED_HEADER]: String(entry.modified_at),
                    [HASH_HEADER]: entry.sha256,
                    'content-length': String(entry.size),
                },
                body_factory: entry.size ? () => this.#manager.read(uri, true) : undefined,
            });
            this._log('UPLOAD', `${uri} - FILE - COMPLETE - ${Date.now() - start}ms`);
        }
        if (response.status === 409) {
            const payload = await response.json().catch(() => ({}));
            if (payload.entry) await this._apply_remote(uri, payload.entry);
        } else {
            const payload = await response.json().catch(() => ({}));
            if (payload.entry && !entries_equivalent(payload.entry, entry))
                await this._apply_remote(uri, payload.entry);
        }
    }

    _plan(remote, local) {
        // Planning is allocation-bounded to one action per effective URI. The
        // authority occupies the first compare_entries argument so it wins ties.
        const actions = new Map();
        const uris = new Set([...Object.keys(remote), ...Object.keys(local)]);
        for (const uri of uris) {
            const server = effective_ref(remote, uri);
            const mirror = effective_ref(local, uri);
            const comparison = compare_entries(server?.entry, mirror?.entry);
            if (comparison === 0) continue;
            if (comparison < 0 && server) {
                const key = `pull:${server.uri}`;
                actions.set(key, {
                    direction: 'pull',
                    uri: server.uri,
                    entry: server.entry,
                    other: effective_ref(local, server.uri)?.entry,
                });
            } else if (comparison > 0 && mirror) {
                const key = `push:${mirror.uri}`;
                actions.set(key, {
                    direction: 'push',
                    uri: mirror.uri,
                    entry: mirror.entry,
                    other: effective_ref(remote, mirror.uri)?.entry,
                });
            }
        }
        return [...actions.values()].sort((left, right) => {
            const priority = action_priority(left.entry) - action_priority(right.entry);
            if (priority) return priority;
            const depth = left.uri.split('/').length - right.uri.split('/').length;
            return depth || left.uri.localeCompare(right.uri);
        });
    }

    async _reconcile_once(remote_manifest) {
        let remote = remote_manifest;
        if (!remote) {
            const response = await this._request('GET', '/v3/manifest');
            const payload = await response.json();
            if (payload.protocol !== PROTOCOL_VERSION) throw new Error('DirectorySync protocol mismatch.');
            remote = payload.manifest;
        }
        const actions = this._plan(remote, this.#map.manifest);
        const perform = (action) => this._schedule(action.uri, () =>
            action.direction === 'pull'
                ? this._apply_remote(action.uri, action.entry)
                : this._push_local(action.uri, action.entry, action.other)
        );
        // Tombstones go first to remove stale type conflicts, directories create
        // parents for files, and a final deepest-first directory metadata pass
        // restores mtimes changed while child files were written.
        for (const action of actions)
            if (action.entry.type === 'tombstone') await perform(action);
        const directories = actions.filter((action) => action.entry.type === 'directory');
        await Promise.all(directories.map(perform));
        await Promise.all(actions.filter((action) => action.entry.type === 'file').map(perform));
        directories.sort((left, right) => right.uri.split('/').length - left.uri.split('/').length);
        for (const action of directories) await perform(action);
    }

    /**
     * Reconciles a complete authority manifest, coalescing overlapping requests.
     *
     * @protected
     * @param {Record<string, import('../../index.js').Entry>} [manifest] Optional already-fetched authority manifest.
     * @param {boolean} [initial=false] Fetch a second manifest to close the startup scan window.
     * @returns {Promise<void>}
     */
    _perform_hard_sync(manifest, initial = false) {
        if (this.#reconcile_promise) {
            this.#reconcile_again = true;
            return this.#reconcile_promise;
        }
        this.#reconcile_promise = (async () => {
            const start = Date.now();
            this._log('HARD_SYNC', 'START');
            await this._reconcile_once(manifest);
            if (initial) await this._reconcile_once();
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

    /** @protected @param {object} message Protocol-v3 mutation event. @returns {void} */
    _receive_mutation(message) {
        if (message.actor === this.#actor || this.#destroyed) return;
        this.#event_chain = this.#event_chain
            .catch(() => undefined)
            .then(() => this._schedule(message.uri, () => this._apply_remote(message.uri, message.entry)));
        this.#event_chain.catch((error) => {
            if (!this.#destroyed) this._emit_error(error);
        });
    }

    /** @protected @param {boolean} reconnect Whether this socket was previously connected. @returns {void} */
    _on_socket_ready(reconnect) {
        this._log('WEBSOCKET', `${reconnect ? 'RECONNECTED' : 'CONNECTED'} - ${this.#options.hostname}:${this.#options.port}`);
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
