import fetch from 'node-fetch';
import Path from 'path';
import Stream from 'stream';
import Websocket from 'ws';
import FileSystem from 'fs/promises';
import EventEmitter from 'events';
import DirectoryMap from './directory/DirectoryMap.js';
import DirectoryManager from './directory/DirectoryManager.js';

import { wrap_object, to_forward_slashes, is_accessible_path, async_for_each, async_wait } from '../utils/operators.js';

// This will hold the shared websocket connections to remote servers
// We will ideally only want a single connection for multiple hosts on the same remote server
const ws_pool = {};

// Bind a global close handler to close all websocket connections
['exit', 'SIGINT', 'SIGUSR1', 'SIGUSR2', 'SIGTERM', 'uncaughtException'].forEach((type) =>
    process.once(type, (arg1) => {
        if (type === 'uncaughtException') console.error(arg1);
        Object.keys(ws_pool).forEach((key) => ws_pool[key].close());
        process.exit();
    })
);

export default class Mirror extends EventEmitter {
    #ws;
    #map;
    #manager;
    #host;
    #options = {
        path: '',
        hostname: '',
        port: 8080,
        ssl: false,
        auth: '',
        retry: {
            every: 1000,
            backoff: true,
        },
    };

    constructor(host, options = this.#options) {
        // Initialize the Event Emitter instance
        super();

        // Ensure the host name is valid
        if (typeof host !== 'string')
            throw new Error('new DirectorySync.Mirror(host, options) -> host must be a valid string.');

        // Wrap default options object with provided options
        if (options === null || typeof options !== 'object')
            throw new Error('new DirectorySync.Mirror(options) -> options must be an object');
        this.#host = host;
        wrap_object(this.#options, options);

        // Initialize local actors
        this._initialize_actors();
    }

    /**
     * Emits a 'log' event with the specified code/message.
     *
     * @private
     * @param {String} code
     * @param {String} message
     */
    _log(code, message) {
        this.emit('log', code, message);
    }

    /**
     * Initialize the DirectoryMap and DirectoryManager instances.
     * @private
     */
    async _initialize_actors() {
        // Translate the user provided path into an absolute system path
        let { path } = this.#options;
        path = to_forward_slashes(Path.resolve(path));

        // Create the directory at path if not specified
        if (!(await is_accessible_path(path))) await FileSystem.mkdir(path);

        // Retrieve remote host map options and schema
        const response = await this._http_request('GET');
        const { options, schema } = await response.json();

        // Initialize a DirectoryMap locally to compare with remote
        options.path = this.#options.path;
        this.#map = new DirectoryMap(options);
        this.#manager = new DirectoryManager(this.#map);

        // Wait for the map to be ready before performing synchronization
        await this.#map.ready();

        // Perform a hard sync to ensure all files/directories are in sync
        await this._perform_hard_sync(schema);

        // Bind mutation listeners to the DirectoryMap
        this._bind_mutation_listeners();

        // Initialize the websocket uplink to receive host events from the remote server
        this._initialize_ws_uplink();
    }

    #ws_cooldown = 0;

    /**
     * Initializes the websocket uplink to receive host events from the remote server.
     * @private
     */
    _initialize_ws_uplink() {
        // Determine a unique hostname:port:host key for sharing ws connections
        const { auth, ssl, hostname, port, retry } = this.#options;
        const identity = `${hostname}:${port}`;
        const is_initial = this.#ws === undefined;
        if (ws_pool[identity]) {
            // Re-use the shared ws connection
            this.#ws = ws_pool[identity];

            // Listen for the recalibrate event to handle disconnections
            this.#ws.once('recalibrate', () => this._initialize_ws_uplink());

            // Subscribe to the remote server's events for this host
            this.#ws.send(
                JSON.stringify({
                    command: 'SUBSCRIBE',
                    host: this.#host,
                })
            );

            // Perform a hard sync to ensure all files/directories are in sync
            if (!is_initial) this._perform_hard_sync();
        } else {
            // Initialize a websocket connection to the remote server
            const reference = this;
            const connection = new Websocket(`${ssl ? 'wss' : 'ws'}://${hostname}:${port}/?auth_key=${auth}`);
            this.#ws = connection;

            // Handle the 'open' event
            connection.on('open', () => {
                // Reset the ws cooldown for future retries
                reference.#ws_cooldown = 0;
                reference._log('WEBSOCKET', `CONNECTED - ${hostname}:${port}`);

                // Subscribe to the remote server's events for this host
                connection.send(
                    JSON.stringify({
                        command: 'SUBSCRIBE',
                        host: reference.#host,
                    })
                );

                // Modify the shared websocket pool connection and recalibrate with consumers
                const current = ws_pool[identity];
                ws_pool[identity] = connection;
                if (current) current.emit('recalibrate');

                // Perform a hard sync to ensure all files/directories are in sync
                if (!is_initial) reference._perform_hard_sync();
            });

            // Handle the 'close' event to handle reconnection
            const { every, backoff } = retry;
            connection.on('close', async () => {
                // Wait for the cooldown to expire before attempting to reconnect
                const cooldown = reference.#ws_cooldown || every;
                reference._log(
                    'WEBSOCKET',
                    `DISCONNECTED - ${hostname}:${port} - ${this.#host} - RETRY[${cooldown}ms]`
                );
                await async_wait(cooldown);

                // Cleanup this cached connection
                delete ws_pool[identity];

                // Retry the connection uplink
                this.#ws_cooldown = backoff ? cooldown * 2 : cooldown;
                reference._initialize_ws_uplink();
            });
        }

        // Handle the 'message' event to handle incoming events
        this.#ws.on('message', (buffer) => {
            const string = buffer.toString();
            console.log(string.split(','));
        });
    }

    /**
     * Binds listeners to all DirectoryMap events for mutating changes to remote.
     * @private
     */
    _bind_mutation_listeners() {
        const reference = this;

        // Bind a listener for 'directory_create' events
        this.#map.on('directory_create', (uri) => this._create_remote_directory(uri));

        // Bind a listener for 'directory_delete' events
        this.#map.on('directory_delete', async (uri) => {
            // Make an HTTP request to delete the directory on the remote server
            const start_time = Date.now();
            await reference._http_request('DELETE', uri);
            reference._log('DELETE', `${uri} - DIRECTORY - ${Date.now() - start_time}ms`);
        });

        // Bind a listener for 'file_create' events
        this.#map.on('file_create', async (uri, { stats }) => {
            // Make an HTTP request to create the directory on the remote server
            const start_time = Date.now();
            await reference._http_request(
                'PUT',
                uri,
                {},
                JSON.stringify([stats.size, stats.created_at, stats.modified_at])
            );
            reference._log('CREATE', `${uri} - FILE - ${Date.now() - start_time}ms`);
        });

        // Bind a listener for 'file_change' events
        this.#map.on('file_change', (uri) => this._upload_file(uri));

        // Bind a listener for 'file_delete' events
        this.#map.on('file_delete', async (uri) => {
            // Make an HTTP request to delete the directory on the remote server
            const start_time = Date.now();
            await reference._http_request('DELETE', uri);
            reference._log('DELETE', `${uri} - FILE - ${Date.now() - start_time}ms`);
        });
    }

    /**
     * Performs a hard files/directories sync with the remote server.
     *
     * @private
     * @param {Object=} schema
     */
    async _perform_hard_sync(schema) {
        // Retrieve a fresh schema from the remote server if one is not provided
        const promises = [];
        const reference = this;
        const start_time = Date.now();
        const local_schema = this.#map.schema;
        if (schema === undefined) {
            // Retrieve remote host map options and schema
            const response = await this._http_request('GET');
            schema = (await response.json()).schema;
        }

        // Perform local->remote synchronization based on schemas
        // We will only sync if the remote server has no record at all for each local record
        await async_for_each(Object.keys(local_schema), async (uri, next) => {
            const remote_record = schema[uri];
            const local_record = local_schema[uri];
            const is_directory = local_record.length === 2;

            // Determine if the remote record is not defined at all
            if (remote_record === undefined) {
                if (is_directory) {
                    // Create a remote directory as it does not exist in remote server
                    await reference._create_remote_directory(uri);
                } else {
                    // Upload the local file to remote server as it does not exist in remote server
                    promises.push(reference._upload_file(uri));
                }
            }

            // Proceed to next record
            next();
        });

        // Perform remote->local synchronization based on schemas
        // We will sync based on whether local or remote is newer calculated by the higher created_at/modified_at timestamp
        await async_for_each(Object.keys(schema), async (uri, next) => {
            // Determine the local and remote schema records
            const remote_record = schema[uri];
            const local_record = local_schema[uri];
            const is_directory = remote_record.length === 2;

            // Handle case where local remote record is a directory
            if (is_directory) {
                // Create the directory if it doesn't exist
                // We want to await this operation as directories are essential before files can be written
                if (local_record === undefined) {
                    await reference.#manager.create(uri, true);
                    reference._log('CREATE', `${uri} - DIRECTORY`);
                }
            } else {
                // Destructure the remote record into components [SIZE, CREATED_AT, MODIFIED_AT]
                const [r_md5, r_cat, r_mat] = remote_record;

                // Determine the sync direction for this file record
                // Sync Direction: 0 - No Sync, 1 - Upload, -1 - Download
                let sync_direction = local_record === undefined ? -1 : 0;
                if (local_record) {
                    // Determine if the MD5 hash of the local file matches the remote file
                    const [l_md5, l_cat, l_mat] = local_record;
                    if (l_md5 !== r_md5) sync_direction = l_cat < r_cat || l_mat < r_mat ? -1 : 1;
                }

                // Perform the sync operation
                switch (sync_direction) {
                    case -1:
                        // Download the file without holding local execution
                        promises.push(reference._download_file(uri));
                        break;
                    case 1:
                        // Upload the file without holding local execution
                        promises.push(reference._upload_file(uri));
                        break;
                }
            }

            // Proceed to next record
            next();
        });

        // Wait for all promises to resolve
        if (promises.length > 0) await Promise.all(promises);

        // Emit a completion log
        this._log('COMPLETE', `HARD_SYNC - ${Date.now() - start_time}ms`);
    }

    /**
     * Makes an HTTP request to the remote server.
     *
     * @private
     * @param {('GET'|'POST'|'PUT'|'DELETE')} method
     * @param {String} uri
     * @param {Object} headers
     * @param {Stream.Readable} body
     * @returns {fetch.Response}
     */
    async _http_request(method, uri, headers, body, retry_delay) {
        // Destructure options to retrive constructor options
        const { hostname, port, ssl, auth } = this.#options;

        // Safely make the HTTP request
        let response;
        try {
            response = await fetch(
                `http${ssl ? 's' : ''}://${hostname}:${port}?host=${encodeURIComponent(this.#host)}${
                    uri ? `&uri=${encodeURIComponent(uri)}` : ''
                }`,
                {
                    method,
                    headers: {
                        ...headers,
                        'x-auth-key': auth,
                    },
                    body,
                }
            );

            // Check if we received a 403 to alert the user
            if (response.status === 403) throw new Error('UNAUTHORIZED');

            // Ensure the response code is non 500x
            if (response.status >= 500 && response.status < 600)
                throw new Error('HTTP request failed with status code: ' + response.status);
        } catch (error) {
            // Throw a higher level error if we received a 403
            if (error.message === 'UNAUTHORIZED') {
                const error = new Error(
                    'Remote Server sent an HTTP 403 meaning your authentication string is invalid.'
                );
                this.emit('error', error);
                throw error;
            }

            // Retry the request if there are some retries remaining
            const { delay, backoff } = this.#options.retry;

            // Wait for the retry delay if one is provided
            const cooldown = retry_delay || delay;
            if (cooldown) await async_wait(cooldown);

            // Retry the request with the updated cooldown
            return await this._http_request(method, uri, headers, body, backoff ? cooldown * 2 : cooldown);
        }

        return response;
    }

    /**
     * Creates a directory record with remote server based on local directory.
     *
     * @param {String} uri
     */
    async _create_remote_directory(uri) {
        // Make an HTTP request to create the directory on the remote server
        const start_time = Date.now();
        const { stats } = this.#map.get(uri);
        const { created_at, modified_at } = stats;
        await this._http_request('PUT', uri, {}, JSON.stringify([created_at, modified_at]));
        this._log('CREATE', `${uri} - DIRECTORY - ${Date.now() - start_time}ms`);
    }

    /**
     * Returns a readable stream for a file from the remote server.
     *
     * @param {String} uri
     * @returns {Promise<Stream.Readable>}
     */
    async _download_file(uri) {
        // Make the HTTP request to retrieve the file stream
        const start_time = Date.now();
        const { status, body } = await this._http_request('GET', uri);

        // Ensure the response status code is valid
        if (status !== 200) throw new Error(`_download_file(${uri}) -> HTTP ${status}`);

        // Write the file stream to local directory
        await this.#manager.write(uri, body);
        this._log('DOWNLOAD', `${uri} - FILE - ${Date.now() - start_time}ms`);
    }

    /**
     * Uploads local file at the specified URI to the remote server.
     *
     * @param {String} uri
     */
    async _upload_file(uri) {
        // Retrieve a readable stream for the file
        const start_time = Date.now();
        const stream = this.#manager.read(uri, true);

        // Make HTTP request to upload the file
        const { stats } = this.#map.get(uri);
        await this._http_request(
            'POST',
            uri,
            {
                'content-length': stats.size.toString(),
            },
            stream
        );
        this._log('UPLOAD', `${uri} - FILE - ${Date.now() - start_time}ms`);
    }
}
