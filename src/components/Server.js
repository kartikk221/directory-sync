const Path = require('path');
const EventEmitter = require('events');
const HyperExpress = require('hyper-express');
const DirectoryMap = require('./directory/DirectoryMap.js');

const { wrap_object, is_accessible_path, to_forward_slashes } = require('../utils/operators.js');

class Server extends EventEmitter {
    #server;
    #hosts = {};
    #options = {
        port: 8080,
        ssl: {
            key: '',
            cert: '',
            passphrase: '',
            dh_params: '',
            prefer_low_memory_usage: false,
        },
        auth: {
            headers: null,
            handler: null,
        },
    };

    constructor(options = this.#options) {
        // Initialize the Event Emitter instance
        super();

        // Wrap default options object with provided options
        if (options === null || typeof options !== 'object')
            throw new Error('new DirectorySync.Server(options) -> options must be an object');
        wrap_object(this.#options, options);

        // Initialize the underlying HyperExpress webserver
        this._initialize_server();
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
     * Initializes the underlying HyperExpress webserver to power this server instance.
     * @private
     */
    _initialize_server() {
        // Spread constructor options for HyperExpress
        const { port, ssl } = this.#options;
        const { key, cert, passphrase, dh_params, prefer_low_memory_usage } = ssl;

        // Create a new HyperExpress.Server instance
        if (key && cert) {
            this.#server = new HyperExpress.Server({
                key_file_name: key,
                cert_file_name: cert,
                passphrase: passphrase,
                dh_params_file_name: dh_params,
                ssl_prefer_low_memory_usage: prefer_low_memory_usage,
            });
        } else {
            this.#server = new HyperExpress.Server();
        }

        // Bind server global uncaught error handler to emitter
        const reference = this;
        this.#server.set_error_handler((request, response, error) => {
            reference.emit('error', error, request);
            return response.status(500).json({
                code: 'UNCAUGHT_ERROR',
                message: 'An uncaught error occured while processing your request.',
            });
        });

        // Bind the appropriate communication routes
        this._bind_http_route();

        // Listen on specified user port
        this.#server
            .listen(port)
            .then(() => this.emit('log', 'STARTUP', `Server started on port ${port}`))
            .catch((error) => this.emit('error', error));
    }

    /**
     * Binds the master HTTP route which will handle all incoming communications.
     * @private
     */
    _bind_http_route() {
        // Create the global catch-all HTTP route
        this.#server.any('/', (request, response) => {
            // Destructure various path/query parameters
            const { host, uri } = request.query_parameters;

            // Ensure that the incoming request maps to a valid host
            if (typeof host !== 'string' || this.#hosts[host] == undefined)
                return response.status(404).json({
                    code: 'INVALID_HOST',
                    message: `The specified host '${typeof host == 'string' ? host : ''}' is invalid.`,
                });

            if (uri === undefined)
                switch (request.method) {
                    case 'GET':
                        break;
                    case 'POST':
                        break;
                    case 'DELETE':
                        break;
                }
        });
    }

    /**
     *
     * @param {String} name
     * @param {String} path
     * @param {import('../components/directory/DirectoryMap').DirectoryMapOptions} options
     */
    async host(name, path, options = {}) {
        // Check if a host with the provided name already exists
        if (this.#hosts[name])
            throw new Error(`DirectorySync.Server.host(name) -> A host with name ${name} already exists`);

        // Translate the user provided path into an absolute system path
        path = to_forward_slashes(Path.resolve(path));

        // Ensure the provided path is a valid directory
        if (!(await is_accessible_path(path)))
            throw new Error(`DirectorySync.Server.host(name, path) -> The provided path ${path} is not accessible`);

        // Create a new DirectoryMap instance for this host
        options.path = path;
        const map = new DirectoryMap(options);

        // Bind a error handler to pass through any errors
        map.on('error', (error) => this.emit('error', error));

        // Wait for the DirectoryMap to be ready
        await map.ready();

        // Create a record for this host instance
        this.#hosts[name] = {
            path,
            map,
        };
    }

    /**
     * Returns the current active hosts for this instance.
     * @returns {Object}
     */
    get hosts() {
        return this.#hosts;
    }

    /**
     * Returns the underlying HyperExpress webserver instance.
     * @returns {HyperExpress.Server}
     */
    get server() {
        return this.#server;
    }

    /**
     * Returns the constructor options for this instance.
     */
    get options() {
        return this.#options;
    }
}

module.exports = Server;
