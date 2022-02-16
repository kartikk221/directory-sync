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

        // Initialize this mirror with remote server
        this._initial_sync();
    }

    /**
     * Compares the provided local file record against remote file record to determine if local record is expired.
     *
     * @param {import('./directory/DirectoryMap.js').MapRecord} local
     * @param {import('./directory/DirectoryMap.js').MapRecord} remote
     * @returns
     */
    _is_file_expired(local, remote) {
        const [l_size, l_cat, l_mat] = local;
        const [r_size, r_cat, r_mat] = remote;
        return l_cat < r_cat || l_mat < r_mat || l_size !== r_size;
    }

    /**
     * Performs initial contents sync with remote server.
     * @private
     */
    async _initial_sync() {
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

        // Perform synchronization of directories/files with remote schema
        const promises = [];
        const reference = this;
        await async_for_each(Object.keys(schema), async (uri) => {
            // Determine the local and remote schema records
            const remote_record = schema[uri];
            const is_directory = remote_record.length === 3; // [SIZE, CREATED_AT, MODIFIED_AT]
            const local_Record = reference.#map.schema[uri];

            // Ensure both remote record and local record exist
            if (remote_record && local_Record) {
                // Ensure the local file was created/modified at later than remote
            }
        });
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
}
