import fetch from 'node-fetch';
import Path from 'path';
import Stream from 'stream';
import FileSystem from 'fs/promises';
import EventEmitter from 'events';
import DirectoryMap from './directory/DirectoryMap.js';
import DirectoryManager from './directory/DirectoryManager.js';

import { wrap_object, to_forward_slashes, is_accessible_path, async_for_each } from '../utils/operators.js';

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
                max: 3,
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

        // Perform a hard sync at the beginning of the instance
        this._perform_hard_sync();
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
     * Performs a hard files/directories sync with the remote server.
     * @private
     */
    async _perform_hard_sync() {
        // Translate the user provided path into an absolute system path
        let { path } = this.#options;
        path = to_forward_slashes(Path.resolve(path));

        // Create the directory at path if not specified
        if (!(await is_accessible_path(path))) await FileSystem.mkdir(path);

        // Retrieve remote host map options and schema
        const start_time = Date.now();
        const response = await this._http_request('GET');
        const { options, schema } = await response.json();

        // Initialize a DirectoryMap locally to compare with remote
        options.path = this.#options.path;
        this.#map = new DirectoryMap(options);
        this.#manager = new DirectoryManager(this.#map);

        // Wait for the map to be ready before performing synchronization
        await this.#map.ready();

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
                let sync_direction = local_record === undefined ? -1 : 0; // -1 = download, 0 = no sync, 1 = upload
                if (local_record) {
                    const [l_size, l_cat, l_mat] = local_record;

                    // Determine if local file record is older than remote
                    if (l_cat < r_cat || l_mat < r_mat) sync_direction = -1;
                    else if (l_cat > r_cat || l_mat > r_mat) sync_direction = 1;
                }

                // Perform the file sync operation
                switch (sync_direction) {
                    case -1:
                        // Download the file from remote server
                        const start_ts = Date.now();
                        const promise = reference._download_file(uri);
                        promises.push(promise);
                        promise.then(() => reference._log('DOWNLOAD', `${uri} - ${Date.now() - start_ts}ms`));
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
     * Creates a websocket connection to the remote server.
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
     * @param {Stream.Readable} body
     * @returns {fetch.Response}
     */
    async _http_request(method, uri, body) {
        // Destructure options to retrive constructor options
        const { hostname, port, ssl, auth } = this.#options;

        // Make the HTTP request
        return await fetch(
            `http${ssl ? 's' : ''}://${hostname}:${port}?host=${encodeURIComponent(this.#host)}${
                uri ? `&uri=${encodeURIComponent(uri)}` : ''
            }`,
            {
                method,
                headers: auth?.headers ? auth?.headers : undefined,
                body,
            }
        );
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
