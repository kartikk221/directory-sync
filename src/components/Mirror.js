import fetch from 'node-fetch';
import Path from 'path';
import Stream from 'stream';
import FileSystem from 'fs/promises';
import EventEmitter from 'events';
import DirectoryMap from './directory/DirectoryMap.js';
import DirectoryManager from './directory/DirectoryManager.js';

import { wrap_object, to_forward_slashes, is_accessible_path, async_for_each, async_wait } from '../utils/operators.js';

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
        auth: {
            headers: null,
        },
        retries: {
            http: {
                max: 5,
                delay: 1000,
            },
            ws: {
                max: Infinity,
                delay: 2500,
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
    }

    /**
     * Binds listeners to all DirectoryMap events for mutating changes to remote.
     * @private
     */
    _bind_mutation_listeners() {
        const reference = this;

        // Bind a listener for 'directory_create' events
        this.#map.on('directory_create', async (uri, { stats }) => {
            // Make an HTTP request to create the directory on the remote server
            const start_time = Date.now();
            await reference._http_request('PUT', uri, {}, JSON.stringify([stats.created_at, stats.modified_at]));
            reference._log('CREATE', `${uri} - DIRECTORY - ${Date.now() - start_time}ms`);
        });

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
        this.#map.on('file_change', async (uri, { stats }) => {
            // Make an HTTP request to create the directory on the remote server
            const start_time = Date.now();
            const stream = reference.#manager.read(uri, true);
            await reference._http_request(
                'POST',
                uri,
                {
                    'content-length': stats.size.toString(),
                },
                stream
            );
            reference._log('UPLOAD', `${uri} - FILE - ${Date.now() - start_time}ms`);
        });

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
        const start_time = Date.now();
        if (schema === undefined) {
            // Retrieve remote host map options and schema
            const response = await this._http_request('GET');
            schema = (await response.json()).schema;
        }

        // Perform synchronization of directories/files with remote schema
        const promises = [];
        const reference = this;
        await async_for_each(Object.keys(schema), async (uri, next) => {
            // Determine the local and remote schema records
            const remote_record = schema[uri];
            const local_record = reference.#map.schema[uri];
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
                const [r_size, r_cat, r_mat] = remote_record;

                // Determine the sync direction for this file record
                let should_download = local_record === undefined;
                if (local_record) {
                    const [l_size, l_cat, l_mat] = local_record;

                    // Determine if local file record is older than remote
                    if (l_cat < r_cat || l_mat < r_mat) should_download = true;
                }

                // Download the file from remote server if it needs to be downloaded
                if (should_download) {
                    const start_ts = Date.now();
                    const download = reference._download_file(uri);
                    promises.push(download);
                    download.then(() => reference._log('DOWNLOAD', `${uri} - FILE - ${Date.now() - start_ts}ms`));
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
     * Creates a websocket connection to the remote server.
     * @private
     */
    _create_ws_connection() {
        // Create websocket connection here
    }

    /**
     * Makes an HTTP request to the remote server.
     *
     * @private
     * @param {('GET'|'POST'|'DELETE')} method
     * @param {String} uri
     * @param {Object} headers
     * @param {Stream.Readable} body
     * @returns {fetch.Response}
     */
    async _http_request(method, uri, headers, body, retries) {
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
                        ...(auth?.headers ? auth?.headers : {}),
                    },
                    body,
                }
            );

            // Ensure the response code is non 500x
            if (response.status >= 500 && response.status < 600)
                throw new Error('HTTP request failed with status code: ' + response.status);
        } catch (error) {
            // Retry the request if there are some retries remaining
            const { max, delay } = this.#options.retries.http;
            const remaining = retries === undefined ? max : retries - 1;
            if (remaining > 0) {
                // Wait for the specified amount of delay
                if (delay > 0) await async_wait(delay);
                return await this._http_request(method, uri, headers, body, remaining);
            }
        }

        return response;
    }

    /**
     * Returns a readable stream for a file from the remote server.
     *
     * @param {String} uri
     * @returns {Promise<Stream.Readable>}
     */
    async _download_file(uri) {
        // Make the HTTP request to retrieve the file stream
        const { status, body } = await this._http_request('GET', uri);

        // Ensure the response status code is valid
        if (status !== 200) throw new Error(`_download_file(${uri}) -> HTTP ${status}`);

        // Write the file stream to local directory
        return await this.#manager.write(uri, body);
    }
}
