import EventEmitter from 'node:events';
import Path from 'node:path';
import HyperExpress from 'hyper-express';
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
import {
    ascii_to_hex,
    canonicalize_uri,
    compare_entries,
    constant_time_equal,
    is_accessible_path,
    merge_options,
    safe_json_parse,
    validate_entry,
} from '../utils/operators.js';

const DEFAULT_OPTIONS = {
    port: 8080,
    auth: '',
    ssl: {
        key: '',
        cert: '',
        passphrase: '',
        dh_params: '',
        prefer_low_memory_usage: false,
    },
    limits: { max_body_length: 100 * 1024 * 1024, fast_buffers: true },
};

/**
 * Central authority for one or more named repositories.
 *
 * Every mutation is validated against the authority's canonical entry before it
 * reaches disk. Newer mtimes win; exact ties retain the authority's entry. Once
 * committed, the canonical entry is published to subscribed mirrors.
 *
 * @extends EventEmitter
 */
export default class Server extends EventEmitter {
    #destroy_promise;
    #destroyed = false;
    #hosts = Object.create(null);
    #options;
    #ready_promise;
    #server;
    #websockets = new Set();

    /**
     * Creates and immediately starts a DirectorySync server.
     *
     * @param {import('../../index.js').ServerOptions} [options={}] HTTP, TLS, authentication, and body-limit options.
     */
    constructor(options = {}) {
        super();
        if (!options || typeof options !== 'object')
            throw new TypeError('new DirectorySync.Server(options) -> options must be an object.');
        this.#options = merge_options(DEFAULT_OPTIONS, options);
        if (!Number.isInteger(this.#options.port) || this.#options.port < 0 || this.#options.port > 65_535)
            throw new TypeError('Server port must be an integer between 0 and 65535.');
        if (typeof this.#options.auth !== 'string') throw new TypeError('Server auth must be a string.');
        this._initialize_server();
    }

    /** @protected @param {string} code Log category. @param {string} message Human-readable detail. @returns {void} */
    _log(code, message) {
        this.emit('log', code, message);
    }

    _initialize_server() {
        const { port, ssl, limits } = this.#options;
        // DirectorySync owns shutdown explicitly. Disabling HyperExpress auto-close
        // prevents hidden process listeners and lets destroy() close maps first.
        const common = { ...limits, auto_close: false };
        this.#server = ssl.key && ssl.cert
            ? new HyperExpress.Server({
                  ...common,
                  key_file_name: ssl.key,
                  cert_file_name: ssl.cert,
                  passphrase: ssl.passphrase,
                  dh_params_file_name: ssl.dh_params,
                  ssl_prefer_low_memory_usage: ssl.prefer_low_memory_usage,
              })
            : new HyperExpress.Server(common);

        this.#server.set_error_handler((request, response, error) => {
            this.emit('error', error, request);
            if (!response.completed)
                return response.status(500).json({ code: 'UNCAUGHT_ERROR', message: 'Request failed.' });
        });
        this._bind_routes();
        this.#ready_promise = this.#server.listen(port).then(() => {
            this._log('STARTUP', `Server started on port ${this.#server.port}`);
            return this;
        });
        this.#ready_promise.catch((error) => this.emit('error', error));
    }

    _protocol(request, response) {
        response.header(PROTOCOL_HEADER, String(PROTOCOL_VERSION));
        if (!constant_time_equal(request.headers[AUTH_HEADER] || '', this.#options.auth)) {
            response.status(403).json({ code: 'UNAUTHORIZED', message: 'Invalid authentication key.' });
            return false;
        }
        if (String(request.headers[PROTOCOL_HEADER] || '') !== String(PROTOCOL_VERSION)) {
            response.status(426).json({
                code: 'PROTOCOL_MISMATCH',
                message: `DirectorySync protocol v${PROTOCOL_VERSION} is required.`,
            });
            return false;
        }
        return true;
    }

    _host(request, response) {
        const name = request.query_parameters?.host;
        const hosted = typeof name === 'string' ? this.#hosts[name] : undefined;
        if (!hosted) {
            response.status(404).json({ code: 'INVALID_HOST', message: `Unknown synchronized host '${name || ''}'.` });
            return undefined;
        }
        return { name, ...hosted };
    }

    _uri(request, response) {
        try {
            return canonicalize_uri(request.query_parameters?.uri);
        } catch (error) {
            response.status(422).json({ code: 'INVALID_URI', message: error.message });
            return undefined;
        }
    }

    _actor(request) {
        const actor = request.headers[ACTOR_HEADER];
        return typeof actor === 'string' && actor.length <= 128 ? actor : 'anonymous';
    }

    _canonical_response(response, hosted, uri, code = 'STALE_MUTATION') {
        return response.status(409).json({ code, entry: hosted.map.canonical_entry(uri) || null });
    }

    _handle_error(response, error, hosted, uri) {
        if (error.code === 'SIZE_LIMIT')
            return response.status(413).json({ code: error.code, message: error.message });
        if (error.code === 'STALE_MUTATION') return this._canonical_response(response, hosted, uri);
        if (['CHECKSUM', 'INVALID_SIZE', 'INVALID_URI', 'TYPE_CONFLICT', 'CONTENT_REQUIRED'].includes(error.code))
            return response.status(422).json({ code: error.code, message: error.message });
        if (error instanceof SyntaxError || error instanceof TypeError)
            return response.status(422).json({ code: 'INVALID_PAYLOAD', message: error.message });
        throw error;
    }

    _route(handler) {
        // Every data route shares the same protocol/authentication/host gate so a
        // handler can operate only on a validated HostedDirectory record.
        return async (request, response) => {
            if (!this._protocol(request, response)) return;
            const hosted = this._host(request, response);
            if (!hosted) return;
            try {
                return await handler.call(this, request, response, hosted);
            } catch (error) {
                const uri = request.query_parameters?.uri;
                return this._handle_error(response, error, hosted, uri);
            }
        };
    }

    _bind_routes() {
        this.#server.any('/', (request, response) =>
            response.status(426).json({
                code: 'PROTOCOL_MISMATCH',
                message: 'This server uses DirectorySync protocol v3. Upgrade both peers.',
            })
        );

        this.#server.get('/v3/manifest', this._route(async (request, response, hosted) => {
            this._log('SCHEMA', `'${hosted.name}' - ${request.ip}`);
            return response.json({
                protocol: PROTOCOL_VERSION,
                options: hosted.map.serializable_options,
                manifest: hosted.map.manifest,
                schema: hosted.map.schema,
            });
        }));

        this.#server.get('/v3/content', this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            const entry = hosted.map.canonical_entry(uri);
            if (!entry) return response.status(404).json({ code: 'NOT_FOUND' });
            if (entry.type !== 'file')
                return response.status(422).json({ code: 'NOT_A_FILE', message: 'Content routes only accept files.' });
            response.header(MODIFIED_HEADER, String(entry.modified_at));
            response.header(HASH_HEADER, entry.sha256);
            response.header('content-length', String(entry.size));
            return response.stream(hosted.manager.read(uri, true), entry.size);
        }));

        this.#server.put('/v3/content', this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            if (!hosted.map.allows(uri, 'file'))
                return response.status(422).json({ code: 'FILTERED', message: 'The authority filters this file.' });
            const entry = {
                type: 'file',
                modified_at: Number(request.headers[MODIFIED_HEADER]),
                size: Number(request.headers['content-length']),
                sha256: request.headers[HASH_HEADER],
            };
            validate_entry(entry);
            if (entry.size > this.#options.limits.max_body_length)
                return response.status(413).json({ code: 'SIZE_LIMIT' });
            const current = hosted.map.canonical_entry(uri);
            // compare_entries is intentionally authority-oriented: a negative result
            // means the existing server entry wins, including exact timestamp ties.
            const comparison = compare_entries(current, entry);
            if (comparison < 0) return this._canonical_response(response, hosted, uri);
            if (comparison === 0) return response.json({ code: 'SUCCESS', entry: current });
            if (
                current?.type === 'file' &&
                current.size === entry.size &&
                current.sha256 === entry.sha256
            ) await hosted.manager.apply_metadata(uri, entry);
            else await hosted.manager.apply_file(uri, entry.size ? request : '', entry, {
                validate_before_commit: () => compare_entries(hosted.map.canonical_entry(uri), entry) >= 0,
            });
            this._publish_mutation(hosted.name, uri, entry, this._actor(request));
            return response.json({ code: 'SUCCESS', entry });
        }));

        this.#server.patch('/v3/content', this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            const entry = await request.json();
            validate_entry(entry);
            if (entry.type !== 'file') throw new TypeError('Expected a file entry.');
            const current = hosted.map.canonical_entry(uri);
            const comparison = compare_entries(current, entry);
            if (comparison < 0) return this._canonical_response(response, hosted, uri);
            if (comparison > 0) {
                await hosted.manager.apply_metadata(uri, entry);
                this._publish_mutation(hosted.name, uri, entry, this._actor(request));
            }
            return response.json({ code: 'SUCCESS', entry });
        }));

        this.#server.put('/v3/directory', this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            if (!hosted.map.allows(uri, 'directory'))
                return response.status(422).json({ code: 'FILTERED', message: 'The authority filters this directory.' });
            const entry = await request.json();
            validate_entry(entry);
            if (entry.type !== 'directory') throw new TypeError('Expected a directory entry.');
            const comparison = compare_entries(hosted.map.canonical_entry(uri), entry);
            if (comparison < 0) return this._canonical_response(response, hosted, uri);
            if (comparison > 0) {
                await hosted.manager.apply_directory(uri, entry);
                this._publish_mutation(hosted.name, uri, entry, this._actor(request));
            }
            return response.json({ code: 'SUCCESS', entry });
        }));

        this.#server.delete('/v3/entry', this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            const entry = await request.json();
            validate_entry(entry);
            if (entry.type !== 'tombstone') throw new TypeError('Expected a tombstone entry.');
            if (!hosted.map.allows(uri, entry.target))
                return response.status(422).json({ code: 'FILTERED', message: 'The authority filters this entry.' });
            const comparison = compare_entries(hosted.map.canonical_entry(uri), entry);
            if (comparison < 0) return this._canonical_response(response, hosted, uri);
            if (comparison === 0) return response.json({ code: 'SUCCESS', entry });
            const applied = await hosted.manager.apply_tombstone(uri, entry);
            this._publish_mutation(hosted.name, uri, applied, this._actor(request));
            return response.json({ code: 'SUCCESS', entry: applied });
        }));

        this.#server.ws('/v3/events', (ws) => {
            this.#websockets.add(ws);
            this._log('WEBSOCKET', `CONNECTED - ${ws.ip}`);
            ws.on('message', (raw) => {
                const message = safe_json_parse(String(raw));
                if (message?.command !== 'SUBSCRIBE' || !this.#hosts[message.host]) return;
                const topic = `events/${ascii_to_hex(message.host)}`;
                if (!ws.is_subscribed(topic)) ws.subscribe(topic);
                // The acknowledgment forms a subscription barrier. Mirrors reconcile
                // after receiving it, so mutations cannot disappear between the HTTP
                // manifest fetch and WebSocket subscription establishment.
                ws.send(JSON.stringify({
                    command: 'SUBSCRIBED',
                    protocol: PROTOCOL_VERSION,
                    host: message.host,
                }));
            });
            ws.on('error', (error) => this.emit('error', error));
            ws.on('close', () => {
                this.#websockets.delete(ws);
                this._log('WEBSOCKET', `DISCONNECTED - ${ws.ip}`);
            });
        });
        this.#server.upgrade('/v3/events', (request, response) => {
            if (!this._protocol(request, response)) return;
            return response.upgrade({ actor: this._actor(request) });
        });
    }

    _publish_mutation(host, uri, entry, actor = 'server') {
        if (!this.#hosts[host]) return;
        this.#server.publish(
            `events/${ascii_to_hex(host)}`,
            JSON.stringify({ command: 'MUTATION', protocol: PROTOCOL_VERSION, host, uri, entry, actor })
        );
    }

    /**
     * Hosts a synchronized directory under a unique authority name.
     *
     * @param {string} name Repository name used by Mirror instances.
     * @param {string} path Local directory that acts as the central authority.
     * @param {import('../../index.js').DirectoryMapOptions} [options={}] Filters, watcher, state, hash, and size options.
     * @returns {Promise<import('../../index.js').HostedDirectory>} The live hosted-directory record.
     */
    async host(name, path, options = {}) {
        await this.ready();
        if (this.#destroyed) throw new Error('DirectorySync.Server has been destroyed.');
        if (typeof name !== 'string' || !name)
            throw new TypeError('DirectorySync.Server.host(name) -> name must be a non-empty string.');
        if (this.#hosts[name]) throw new Error(`A host with name '${name}' already exists.`);
        const root = Path.resolve(path);
        if (!(await is_accessible_path(root, { directory: true })))
            throw new Error(`The provided path is not an accessible directory: ${root}`);

        const map_options = merge_options({}, options);
        map_options.path = root;
        map_options.limits ||= {};
        map_options.limits.max_file_size ??= this.#options.limits.max_body_length;
        if (map_options.limits.max_file_size > this.#options.limits.max_body_length)
            throw new Error("The host's max_file_size exceeds the server's max_body_length.");
        const map = new DirectoryMap(map_options);
        const manager = new DirectoryManager(map, true);
        map.on('error', (error) => this.emit('error', error));
        map.on('log', (code, message) => this._log(code, `'${name}' - ${message}`));
        map.on('file_size_limit', (uri, record) =>
            this._log('SIZE_LIMIT_REACHED', `${uri} - SIZE_${record.stats.size}_BYTES`)
        );
        await map.ready();
        map.on('mutation', (uri, entry) => this._publish_mutation(name, uri, entry, 'server'));
        this.#hosts[name] = { path: root, map, manager };
        return this.#hosts[name];
    }

    /**
     * Stops hosting a repository and closes its filesystem watcher.
     *
     * @param {string} name Repository name previously passed to host().
     * @returns {Promise<boolean>} True when a repository was removed; false when it was unknown.
     */
    async unhost(name) {
        const hosted = this.#hosts[name];
        if (!hosted) return false;
        delete this.#hosts[name];
        await hosted.map.destroy();
        return true;
    }

    /**
     * Waits until HyperExpress has bound the listening socket.
     *
     * @returns {Promise<Server>} This server instance.
     */
    ready() {
        return this.#ready_promise;
    }

    /**
     * Gracefully closes HTTP, WebSocket, and filesystem resources. Idempotent.
     *
     * @returns {Promise<void>}
     */
    destroy() {
        if (this.#destroy_promise) return this.#destroy_promise;
        this.#destroyed = true;
        this.#destroy_promise = (async () => {
            await this.#ready_promise.catch(() => undefined);
            await this.#server.shutdown();
            for (const websocket of this.#websockets) websocket.close(1001, 'Server shutting down');
            this.#websockets.clear();
            await Promise.all(Object.keys(this.#hosts).map((name) => this.unhost(name)));
        })();
        return this.#destroy_promise;
    }

    /** @returns {Record<string, import('../../index.js').HostedDirectory>} Live repositories keyed by host name. */
    get hosts() { return this.#hosts; }

    /** @returns {HyperExpress.Server} Underlying HyperExpress v7 server. */
    get server() { return this.#server; }

    /** @returns {import('../../index.js').ServerOptions} Effective server options. */
    get options() { return this.#options; }

    /** @returns {boolean} Whether destroy() has started. */
    get destroyed() { return this.#destroyed; }
}
