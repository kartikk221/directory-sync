import EventEmitter from 'node:events';
import { randomUUID } from 'node:crypto';
import Path from 'node:path';
import HyperExpress from 'hyper-express';
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
import { KeyedQueue } from '../utils/queues.js';
import {
    ascii_to_hex,
    canonicalize_uri,
    compare_entries,
    constant_time_equal,
    entries_equivalent,
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
 * reaches disk. Matching revisions establish causal order, while timestamps
 * resolve stale or unknown history. Committed entries receive a new revision
 * and are published to subscribed mirrors.
 *
 * @extends EventEmitter
 */
export default class Server extends EventEmitter {
    #authorities = Object.create(null);
    #destroy_promise;
    #destroyed = false;
    #epoch = randomUUID();
    #hosts = Object.create(null);
    #options;
    #ready_promise;
    #revision = 0;
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
        this.#ready_promise = this.#server.listen(port).then(() => this);
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
        return { name, ...hosted, authority: this.#authorities[name] };
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

    _next_revision() {
        if (this.#revision >= Number.MAX_SAFE_INTEGER)
            throw new Error('The Server authority revision limit was reached. Restart the Server to begin a new epoch.');
        return ++this.#revision;
    }

    _tombstone_key(uri, target) {
        return `${target}:${uri}`;
    }

    _newer_record(left, right) {
        if (!left) return right;
        if (!right) return left;
        if (left.revision !== right.revision)
            return left.revision > right.revision ? left : right;
        return compare_entries(left.entry, right.entry) === 1 ? right : left;
    }

    _tombstone_record(hosted, uri) {
        let selected;
        for (const target of ['file', 'directory']) {
            const record = hosted.map.state.get_tombstone_record(uri, target);
            if (!record || record.entry.include_self === false) continue;
            selected = this._newer_record(selected, {
                ...record,
                revision: hosted.authority.tombstones.get(this._tombstone_key(record.uri, target)) || 0,
            });
        }
        let cursor = Path.posix.dirname(uri);
        while (cursor && cursor !== '/') {
            const record = hosted.map.state.get_tombstone_record(cursor, 'directory');
            if (record) selected = this._newer_record(selected, {
                ...record,
                revision: hosted.authority.tombstones.get(this._tombstone_key(record.uri, 'directory')) || 0,
            });
            const parent = Path.posix.dirname(cursor);
            cursor = parent === cursor ? '/' : parent;
        }
        return selected;
    }

    _authority_record(hosted, uri) {
        const entry = hosted.map.get_entry(uri);
        const active = entry ? {
            uri,
            entry,
            revision: hosted.authority.active.get(uri) || 0,
        } : undefined;
        const tombstone = this._tombstone_record(hosted, uri);
        if (
            active &&
            tombstone?.entry.target === 'directory' &&
            tombstone.entry.include_self === false
        ) return active;
        return this._newer_record(active, tombstone);
    }

    _base_revision(request) {
        if (request.headers[EPOCH_HEADER] !== this.#epoch) return undefined;
        const value = request.headers[BASE_REVISION_HEADER];
        if (value === undefined) return undefined;
        const revision = Number(value);
        if (!Number.isSafeInteger(revision) || revision < 0)
            throw new TypeError('The base revision must be a non-negative safe integer.');
        return revision;
    }

    _record_mutation(hosted, uri, entry) {
        const revision = this._next_revision();
        if (entry.type === 'tombstone') {
            hosted.authority.tombstones.set(this._tombstone_key(uri, entry.target), revision);
            if (entry.target === 'directory') {
                for (const candidate of hosted.authority.active.keys())
                    if (
                        (candidate === uri || candidate.startsWith(`${uri}/`)) &&
                        !hosted.map.get_entry(candidate)
                    ) hosted.authority.active.delete(candidate);
            } else if (!hosted.map.get_entry(uri)) hosted.authority.active.delete(uri);
        } else hosted.authority.active.set(uri, revision);
        if (hosted.authority.tombstones.size > 1_024 && revision % 1_024 === 0) {
            const records = hosted.map.tombstones;
            if (hosted.authority.tombstones.size > records.length * 2) {
                const retained = new Set();
                for (const record of records)
                    retained.add(this._tombstone_key(record.uri, record.entry.target));
                for (const key of hosted.authority.tombstones.keys())
                    if (!retained.has(key)) hosted.authority.tombstones.delete(key);
            }
        }
        return { uri, entry, revision };
    }

    _success(response, record) {
        return response.json({
            code: 'SUCCESS',
            epoch: this.#epoch,
            entry: record?.entry || null,
            revision: record?.revision ?? 0,
        });
    }

    _conflict(response, record, code = 'STALE_MUTATION') {
        return response.status(409).json({
            code,
            epoch: this.#epoch,
            entry: record?.entry || null,
            revision: record?.revision ?? 0,
        });
    }

    _mutate(hosted, uri, entry, request, apply) {
        return hosted.authority.work.run(uri, async () => {
            const current = this._authority_record(hosted, uri);
            if (current && entries_equivalent(current.entry, entry))
                return { accepted: false, conflict: false, record: current };
            const base = this._base_revision(request);
            if (
                current?.entry.type === 'file' && entry.type === 'file' &&
                current.entry.size === entry.size && current.entry.sha256 === entry.sha256 &&
                base !== current.revision
            ) return { accepted: false, conflict: false, record: current };
            const comparison = current && base === current.revision
                ? 1
                : compare_entries(current?.entry, entry);
            if (comparison < 0) return { accepted: false, conflict: true, record: current };
            if (comparison === 0) return { accepted: false, conflict: false, record: current };
            const validate = () => {
                const latest = this._authority_record(hosted, uri);
                return (!latest && !current) || (
                    latest?.revision === current?.revision &&
                    entries_equivalent(latest?.entry, current?.entry)
                );
            };
            const applied = await apply(validate, current?.entry);
            const record = this._record_mutation(hosted, uri, applied);
            this._publish_mutation(hosted.name, record, this._actor(request));
            return { accepted: true, conflict: false, record };
        });
    }

    _canonical_response(response, hosted, uri, code = 'STALE_MUTATION') {
        return this._conflict(response, this._authority_record(hosted, uri), code);
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
                message: `This server uses DirectorySync protocol v${PROTOCOL_VERSION}. Upgrade both peers.`,
            })
        );

        this.#server.get(`${PROTOCOL_PATH}/manifest`, this._route(async (request, response, hosted) => {
            const manifest = hosted.map.manifest;
            const manifest_uris = Object.keys(manifest);
            const versions = {};
            for (const uri of manifest_uris)
                versions[uri] = hosted.authority.active.get(uri) || 0;
            const tombstones = hosted.map.tombstones;
            for (const record of tombstones)
                record.revision = hosted.authority.tombstones.get(
                    this._tombstone_key(record.uri, record.entry.target)
                ) || 0;
            this._log('MANIFEST', `${request.ip} - '${hosted.name}' - ${manifest_uris.length} ENTRIES - ${tombstones.length} TOMBSTONES`);
            return response.json({
                epoch: this.#epoch,
                protocol: PROTOCOL_VERSION,
                server_time: Date.now(),
                filters: {
                    keep: typeof hosted.map.options.filters.keep === 'function'
                        ? null
                        : hosted.map.options.filters.keep,
                    ignore: typeof hosted.map.options.filters.ignore === 'function'
                        ? null
                        : hosted.map.options.filters.ignore,
                },
                manifest,
                tombstones,
                versions,
            });
        }));

        this.#server.get(`${PROTOCOL_PATH}/content`, this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            const record = this._authority_record(hosted, uri);
            const entry = record?.entry;
            if (!entry) return response.status(404).json({ code: 'NOT_FOUND' });
            if (entry.type !== 'file')
                return response.status(422).json({ code: 'NOT_A_FILE', message: 'Content routes only accept files.' });
            response.header(MODIFIED_HEADER, String(entry.modified_at));
            response.header(HASH_HEADER, entry.sha256);
            response.header(EPOCH_HEADER, this.#epoch);
            response.header(REVISION_HEADER, String(record?.revision ?? 0));
            response.header('content-length', String(entry.size));
            const started = Date.now();
            const detail = `${request.ip} - '${hosted.name}' - ${uri} - FILE - ${entry.size} BYTES`;
            const stream = hosted.manager.read(uri, true);
            this._log('UPLOAD', `${detail} - START`);
            stream.once('close', () => {
                if (stream.bytesRead === entry.size)
                    this._log('UPLOAD', `${detail} - COMPLETE - ${Date.now() - started}ms`);
            });
            return response.stream(stream, entry.size);
        }));

        this.#server.put(`${PROTOCOL_PATH}/content`, this._route(async (request, response, hosted) => {
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
            const result = await this._mutate(hosted, uri, entry, request, async (validate, current) => {
                if (
                    current?.type === 'file' &&
                    current.size === entry.size &&
                    current.sha256 === entry.sha256
                ) return (await hosted.manager.apply_metadata(uri, entry)).entry;
                const started = Date.now();
                const detail = `${request.ip} - '${hosted.name}' - ${uri} - FILE - ${entry.size} BYTES`;
                this._log('DOWNLOAD', `${detail} - START`);
                const applied = await hosted.manager.apply_file(uri, entry.size ? request : '', entry, {
                    validate_before_commit: validate,
                });
                this._log('DOWNLOAD', `${detail} - COMPLETE - ${Date.now() - started}ms`);
                return applied.entry;
            });
            return result.conflict
                ? this._conflict(response, result.record)
                : this._success(response, result.record);
        }));

        this.#server.patch(`${PROTOCOL_PATH}/content`, this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            const entry = await request.json();
            validate_entry(entry);
            if (entry.type !== 'file') throw new TypeError('Expected a file entry.');
            const result = await this._mutate(hosted, uri, entry, request, async () =>
                (await hosted.manager.apply_metadata(uri, entry)).entry
            );
            return result.conflict
                ? this._conflict(response, result.record)
                : this._success(response, result.record);
        }));

        this.#server.put(`${PROTOCOL_PATH}/directory`, this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            if (!hosted.map.allows(uri, 'directory'))
                return response.status(422).json({ code: 'FILTERED', message: 'The authority filters this directory.' });
            const entry = await request.json();
            validate_entry(entry);
            if (entry.type !== 'directory') throw new TypeError('Expected a directory entry.');
            const result = await this._mutate(hosted, uri, entry, request, async () =>
                (await hosted.manager.apply_directory(uri, entry)).entry
            );
            return result.conflict
                ? this._conflict(response, result.record)
                : this._success(response, result.record);
        }));

        this.#server.delete(`${PROTOCOL_PATH}/entry`, this._route(async (request, response, hosted) => {
            const uri = this._uri(request, response);
            if (!uri) return;
            const entry = await request.json();
            validate_entry(entry);
            if (entry.type !== 'tombstone') throw new TypeError('Expected a tombstone entry.');
            if (!hosted.map.allows(uri, entry.target))
                return response.status(422).json({ code: 'FILTERED', message: 'The authority filters this entry.' });
            const result = await this._mutate(hosted, uri, entry, request, () =>
                hosted.manager.apply_tombstone(uri, entry)
            );
            return result.conflict
                ? this._conflict(response, result.record)
                : this._success(response, result.record);
        }));

        this.#server.ws(`${PROTOCOL_PATH}/events`, (ws) => {
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
        this.#server.upgrade(`${PROTOCOL_PATH}/events`, (request, response) => {
            if (!this._protocol(request, response)) return;
            return response.upgrade({ actor: this._actor(request) });
        });
    }

    _publish_mutation(host, record, actor = 'server') {
        if (!this.#hosts[host]) return;
        this.#server.publish(
            `events/${ascii_to_hex(host)}`,
            JSON.stringify({
                command: 'MUTATION',
                protocol: PROTOCOL_VERSION,
                epoch: this.#epoch,
                host,
                uri: record.uri,
                entry: record.entry,
                revision: record.revision,
                actor,
            })
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
            this._log('SIZE_LIMIT_REACHED', `'${name}' - ${uri} - SIZE_${record.stats.size}_BYTES`)
        );
        await map.ready();
        const hosted = { path: root, map, manager };
        const authority = {
            active: new Map(),
            tombstones: new Map(),
            work: new KeyedQueue(),
        };
        const internal = { name, ...hosted, authority };
        map.on('mutation', (uri, entry) => {
            const record = this._record_mutation(internal, uri, entry);
            this._publish_mutation(name, record, 'server');
        });
        this.#authorities[name] = authority;
        this.#hosts[name] = hosted;
        return hosted;
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
        const authority = this.#authorities[name];
        delete this.#hosts[name];
        delete this.#authorities[name];
        authority.work.close();
        await authority.work.idle();
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
            const hosts = [];
            for (const name of Object.keys(this.#hosts)) hosts.push(this.unhost(name));
            await Promise.all(hosts);
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
