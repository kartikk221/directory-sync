import fetch from 'node-fetch';
import Path from 'path';
import Stream from 'stream';
import Websocket from 'ws';
import JustQueue from 'just-queue';
import FileSystem from 'fs/promises';
import EventEmitter from 'events';
import DirectoryMap from './directory/DirectoryMap.js';
import DirectoryManager from './directory/DirectoryManager.js';

import {
    wrap_object,
    to_forward_slashes,
    is_accessible_path,
    async_for_each,
    async_wait,
    safe_json_parse,
} from '../utils/operators.js';

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
    #host;
    #queue;
    #manager;
    #destroyed = false;
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
        queue: {
            max_concurrent: 10,
            max_queued: Infinity,
            timeout: Infinity,
            throttle: {
                rate: Infinity,
                interval: Infinity,
            },
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

        // Initialize the mirror instance
        this.#host = host;
        wrap_object(this.#options, options);

        // Initialize the network queue
        this.#queue = new JustQueue(this.#options.queue);

        // Initialize local actors
        this._initialize_actors();
    }

    /**
     * Destroys this Mirror instance.
     *
     * @returns {Promise}
     */
    async destroy() {
        // Destroy the DirectoryMap instance
        await this.#map.destroy();

        // Mark this instance as destroyed
        this.#destroyed = true;
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
        this.#manager = new DirectoryManager(this.#map, true);

        // Bind a 'error' handler to pipe errors
        this.#map.on('error', (error) => this.emit('error', error));

        // Bind a 'file_size_limit' handler to log the event
        this.#map.on('file_size_limit', (uri, object) =>
            this._log(
                'SIZE_LIMIT_REACHED',
                `${uri} - SIZE_${object.stats.size}_BYTES > LIMIT_${this.#map.options.limits.max_file_size}_BYTES`
            )
        );

        // Wait for the map to be ready before performing synchronization
        await this.#map.ready();

        // Perform a hard sync to ensure all files/directories are in sync
        await this._perform_hard_sync(schema);

        // Bind mutation listeners to the DirectoryMap
        this._bind_mutation_listeners();

        // Initialize the websocket uplink to receive host events from the remote server
        this._initialize_ws_uplink();
    }

    #ws_cooldown;

    /**
     * Initializes the websocket uplink to receive host events from the remote server.
     *
     * @private
     * @param {Boolean} pooling
     */
    async _initialize_ws_uplink(pooling = true) {
        // Determine a unique hostname:port:host key for sharing ws connections
        const reference = this;
        const { auth, ssl, hostname, port, retry } = this.#options;
        const identity = `${hostname}:${port}`;
        const is_initial = this.#ws === undefined;
        if (pooling && ws_pool[identity]) {
            // Re-use the shared ws connection
            this.#ws = ws_pool[identity];

            // Wait for the pooled connection to either open or close
            await new Promise((resolve) => {
                // Create a passthrough resolve reference which we can nullify  after use
                let _resolve = resolve;

                // If the connection was already opened, then resolve immediately
                if (reference.#ws.readyState === Websocket.OPEN) {
                    _resolve();
                    _resolve = null;
                }

                // Listen for the 'open' event to know when the connection is ready
                reference.#ws.on('open', () => {
                    // Resolve the state promise if it has not already been resolved
                    if (_resolve) {
                        _resolve();
                        _resolve = null;
                    }
                });

                // Listen for the 'close' event to handle reconnection
                const { every, backoff } = retry;
                reference.#ws.once('close', async () => {
                    // Resolve the state promise if it has not already been resolved
                    if (_resolve) {
                        _resolve();
                        _resolve = null;
                    }

                    // Wait for the cooldown to expire before attempting to reconnect
                    const cooldown = reference.#ws_cooldown || every;
                    reference._log(
                        'WEBSOCKET',
                        `DISCONNECTED - ${hostname}:${port} - ${reference.#host}${
                            reference.#destroyed ? '' : ` - RETRY[${cooldown}ms`
                        }]`
                    );

                    // Do not continue the retry cycle if instance is destroyed
                    if (reference.#destroyed) return;

                    // Retry the connection uplink after waiting the cooldown milliseconds
                    if (cooldown > 0) await async_wait(cooldown + 1);
                    reference.#ws_cooldown = backoff ? cooldown * 2 : cooldown;
                    reference._initialize_ws_uplink();
                });
            });

            // Ensure the pooled connection successfully opened
            if (this.#ws.readyState === Websocket.OPEN) {
                // Subscribe to the remote server's events for this host
                reference.#ws.send(
                    JSON.stringify({
                        command: 'SUBSCRIBE',
                        host: this.#host,
                    })
                );

                // Perform a hard sync to ensure all files/directories are in sync
                if (!is_initial) reference._perform_hard_sync();
            }
        } else {
            // Initialize a websocket connection to the remote server
            const reference = this;
            const connection = new Websocket(`${ssl ? 'wss' : 'ws'}://${hostname}:${port}/?auth_key=${auth}`);
            this.#ws = connection;

            // Store the ws connection in the ws pool
            ws_pool[identity] = connection;

            // Handle the 'open' event
            connection.once('open', () => {
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

                // Perform a hard sync to ensure all files/directories are in sync
                if (!is_initial) reference._perform_hard_sync();
            });

            // Handle the 'error' event
            connection.once('error', (error) => this.emit('error', error));

            // Handle the 'close' event to handle reconnection
            const { every, backoff } = retry;
            connection.once('close', async () => {
                // Wait for the cooldown to expire before attempting to reconnect
                const cooldown = reference.#ws_cooldown || every;
                reference._log(
                    'WEBSOCKET',
                    `DISCONNECTED - ${hostname}:${port} - ${reference.#host}${
                        reference.#destroyed ? '' : ` - RETRY[${cooldown}ms`
                    }]`
                );

                // Do not continue the retry cycle if instance is destroyed
                if (reference.#destroyed) return;

                // Retry the connection uplink after waiting the cooldown milliseconds
                if (cooldown > 0) await async_wait(cooldown);
                reference.#ws_cooldown = backoff ? cooldown * 2 : cooldown;
                reference._initialize_ws_uplink(false);
            });
        }

        // Handle the 'message' event to handle incoming events
        this.#ws.on('message', (buffer) => {
            // Safely parse incoming command as JSON and handle it based message.command
            const message = safe_json_parse(buffer.toString());
            if (message)
                switch (message.command) {
                    case 'MUTATION':
                        // Handle the incoming mutation event
                        const { host, type, uri, md5, is_directory } = message;

                        // Ensure the incoming event is for this host as we may be sharing the websocket connection
                        if (this.#host === host) reference._handle_remote_mutation(type, uri, md5, is_directory);
                        break;
                }
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
     * Handles incoming remote MUTATION command to synchronize remote->local.
     *
     * @param {('CREATE'|'MODIFIED'|'DELETE')} type
     * @param {String} uri
     * @param {String} md5
     * @param {Boolean} is_directory
     */
    async _handle_remote_mutation(type, uri, md5, is_directory = true) {
        // Handle mutation event if it's actor id does not match the self id
        switch (type) {
            case 'CREATE':
                // Create the directory/file with local manager
                this.#manager.create(uri, is_directory);
                this._log('CREATE', `${uri} - SERVER - ${is_directory ? 'DIRECTORY' : 'FILE'}`);
                break;
            case 'MODIFIED':
                // Compare the incoming md5 with the local md5
                const record = this.#map.get(uri);
                if (record) {
                    // If the local_md5 matches incoming md5, skip the mutation
                    const local_md5 = record.stats.md5;
                    if (local_md5 === md5) return;
                }

                // Download the file from remote as it has been modified
                this._download_file(uri);
                break;
            case 'DELETE':
                // Delete the directory/file with local manager
                this.#manager.delete(uri, is_directory);
                this._log('DELETE', `${uri} - SERVER - ${is_directory ? 'DIRECTORY' : 'FILE'}`);
                break;
        }
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
        this._log('HARD_SYNC', `START`);
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
                    if (l_md5 !== r_md5) {
                        // Give priority to the modified at timestamp if the MD5 hashes do not match
                        // Fall back to created at timestamp if modified at timestamp is not available as it is less accurate but is a last resort
                        if (l_mat && r_mat) {
                            sync_direction = l_mat < r_mat ? -1 : 1;
                        } else if (l_cat && r_cat) {
                            sync_direction = l_cat < r_cat ? -1 : 1;
                        }
                    }
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

        // Wait for all promises to resolve if we have some promises
        if (promises.length > 0) await Promise.all(promises);

        // Emit a completion log
        this._log('HARD_SYNC', `COMPLETE - ${Date.now() - start_time}ms`);
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
            // Queue the HTTP request in the Network Queue to prevent extensive backpressure
            response = await this.#queue.queue(() =>
                fetch(
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
                )
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
            const { every, backoff } = this.#options.retry;

            // Wait for the retry delay if one is provided
            const cooldown = retry_delay || every;
            if (cooldown) {
                this._log('HTTP', `ERROR - ${method} - ${error.message}`);
                await async_wait(cooldown);
            }

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
        this._log('CREATE', `${uri} - REMOTE_DIRECTORY - START`);
        await this._http_request('PUT', uri, {}, JSON.stringify([created_at, modified_at]));
        this._log('CREATE', `${uri} - REMOTE_DIRECTORY - ${Date.now() - start_time}ms`);
    }

    /**
     * Returns a readable stream for a file from the remote server.
     *
     * @param {String} uri
     * @returns {Promise<Stream.Readable>}
     */
    async _download_file(uri, delay) {
        // Make the HTTP request to retrieve the file stream
        const start_time = Date.now();
        this._log('DOWNLOAD', `${uri} - FILE - START`);
        const { status, body, headers } = await this._http_request('GET', uri);

        // Perform a follow-up hard sync if we recieve a 404 after retry delay
        if (status === 404) return this._log('DOWNLOAD', `${uri} - FILE - 404`);

        // Ensure the response status code is valid
        if (status !== 200) throw new Error(`_download_file(${uri}) -> HTTP ${status}`);

        // Stream the file to the local file system if we receive some content else empty the file
        try {
            // Supress the indirect_write to prevent an unneccessary upload
            const expected_md5 = headers.get('md5-hash') || '';
            await this.#manager.indirect_write(
                uri,
                +headers.get('content-length') === 0 ? '' : body,
                expected_md5,
                true
            );
        } catch (error) {
            // Wait for the appropriate cooldown delay before retrying download
            const { every, backoff } = this.#options.retry;
            const cooldown = delay || every;
            this._log('DOWNLOAD', `${uri} - FILE - FAILED - RETRY - ${cooldown}ms`);
            this.emit('error', error);
            await async_wait(cooldown);

            // Retry the download with the updated cooldown
            return await this._download_file(uri, backoff ? cooldown * 2 : cooldown);
        }

        this._log('DOWNLOAD', `${uri} - FILE - COMPLETE - ${Date.now() - start_time}ms`);
    }

    /**
     * Uploads local file at the specified URI to the remote server.
     *
     * @param {String} uri
     */
    async _upload_file(uri, delay) {
        // Retrieve a readable stream for the file
        const start_time = Date.now();
        this._log('UPLOAD', `${uri} - FILE - START`);
        const stream = this.#manager.read(uri, true);

        // Make HTTP request to upload the file
        const { stats } = this.#map.get(uri);
        const { status } = await this._http_request(
            'POST',
            uri,
            {
                'content-length': stats.size.toString(),
                'md5-hash': stats.md5,
            },
            stream
        );

        // Determine if a retry is required due to delivery failure
        if (status === 409) {
            // Wait for the appropriate cooldown delay
            const { every, backoff } = this.#options.retry;
            const cooldown = delay || every;
            this._log('UPLOAD', `${uri} - FILE - FAILED - RETRY - ${cooldown}ms`);
            await async_wait(cooldown);

            // Retry the upload with the updated cooldown
            return await this._upload_file(uri, backoff ? cooldown * 2 : cooldown);
        }

        // Ensure the response status code is valid
        if (status !== 200) throw new Error(`_upload_file(${uri}) -> HTTP ${status}`);

        this._log('UPLOAD', `${uri} - FILE - END - ${Date.now() - start_time}ms`);
    }

    /* Mirror Instance Getters */

    /**
     * Returns whether this mirror instance is destroyed.
     * @returns {Boolean}
     */
    get destroyed() {
        return this.#destroyed;
    }
}
