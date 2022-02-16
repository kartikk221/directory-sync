const fetch = require('node-fetch');
const Path = require('path');
const Stream = require('stream');
const FileSystem = require('fs/promises');
const EventEmitter = require('events');
const DirectoryMap = require('./directory/DirectoryMap.js');

const { wrap_object, to_forward_slashes } = require('../utils/operators');

class Mirror extends EventEmitter {
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
     * Performs initial contents sync with remote server.
     * @private
     */
    async _initial_sync() {
        // Translate the user provided path into an absolute system path
        let { path } = this.#options;
        path = to_forward_slashes(Path.resolve(path));

        // Create the directory at path if not specified
        if (!(await is_accessible_path(path))) await FileSystem.mkdir(path);

        // Create a new DirectoryMap instance for this mirror
        options.path = path;
        this.#map = new DirectoryMap(options);
        this.#manager = new DirectoryManager(this.#map);

        // Bind a error handler to pass through any errors
        map.on('error', (error) => this.emit('error', error));

        // Wait for the DirectoryMap to be ready
        await map.ready();

        // Make a request to remote server to retrieve schema
        const local_schema = map.schema;
        const response = await this._http_request('GET');
        const remote_schema = await response.json();

        console.log(local_schema);
        console.log(remote_schema);
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

module.exports = Mirror;
