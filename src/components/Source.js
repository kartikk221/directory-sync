const Path = require('path');
const Etag = require('etag');
const EventEmitter = require('events');
const HyperExpress = require('hyper-express');
const LiveDirectory = require('live-directory');
const {
    wrap_object,
    accessible_path,
    write_file,
    delete_path,
    to_forward_slashes,
    create_directory,
} = require('../shared/operators');

class Source extends EventEmitter {
    #server;
    #directories = {};
    #identifiers = {};
    #identities = {};
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

    constructor(options) {
        super();

        // Wrap default options object with provided options
        if (options === null || typeof options !== 'object')
            throw new Error('DirectorySync.Host.options must be an object');
        wrap_object(this.#options, options);

        // Initiate HyperExpress server instance
        this._initiate_server();
    }

    /**
     * Emits formatted log event with a context and message.
     *
     * @private
     * @param {String} context
     * @param {String} message
     */
    _log(context, message) {
        this.emit('log', `[${context}] ${message}`);
    }

    /**
     * @private
     * Initiates HyperExpress server instance.
     */
    _initiate_server() {
        // Spread options values for HyperExpress
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

        // Bind server error handler to emitter
        let reference = this;
        this.#server.set_error_handler((request, response, error) => {
            reference.emit('error', error);
            return response.status(500).json({
                code: 'OPERATION_FAILED',
                message: error.message,
                stack: error.stack || error.trace,
            });
        });

        // Bind events route
        this._initate_route();

        // Listen on specified user port
        this.#server
            .listen(port)
            .then(() => this._log('STARTUP', `STARTED_SERVER >> [PORT] >> [${port}]`))
            .catch((error) => this.emit('error', error));
    }

    /**
     * @private
     * Initiates listener route for handling network events.
     */
    _initate_route() {
        this.#server.post('/:identity/:action', (request, response) =>
            this._authenticate_request(request, response)
        );
    }

    /**
     * @private
     * Authenticates incoming requests from external sources
     */
    async _authenticate_request(request, response) {
        // Destructure authentication modes
        const { headers, handler } = this.#options.auth;

        // Compare headers of incoming request for security
        if (headers && typeof headers == 'object') {
            let verdict = true;
            let incoming = request.headers;
            Object.keys(headers).forEach((name) => {
                const value = headers[name];
                if (incoming[name] !== value) verdict = false;
            });

            // Abort the request instantly without a response code
            if (!verdict) return response.close();
        }

        // Execute user specified auth handler if a true verdict isn't returned
        if (handler && typeof handler == 'function') {
            const verdict = await handler(request, response);
            if (verdict !== true) return response.close();
        }

        // Everything is good to go, onto the request handler
        this._handle_request(request, response);
    }

    /**
     * Returns unique identifier for an incoming request based on it's IP.
     *
     * @param {Request} request
     * @returns {String}
     */
    _get_identifier(request) {
        // Check cache for a Etag of the IP
        const ip = request.ip;
        if (this.#identifiers[ip]) return this.#identifiers[ip];

        // Cache and return Etag of IP
        this.#identifiers[ip] = Etag(ip).split('"').join('');
        return this.#identifiers[ip];
    }

    /**
     * Handles incoming requests from HyperExpress server route.
     *
     * @private
     * @param {HyperExpress.Request} request
     * @param {HyperExpress.Response} response
     */
    async _handle_request(request, response) {
        // Destructure properties from incoming request
        const { identity, action } = request.path_parameters;

        // Return a 404 if provided identity is not being hosted
        const object = this.#identities[identity];
        if (object == undefined)
            return response.status(404).json({
                code: 'INVALID_IDENTITY',
                message: `The identity ${identity} is not being hosted by this DirectorySync.Host instance.`,
            });

        // Generate unique identifier for incoming request
        const identifier = this._get_identifier(request);

        // Handle 'OPTIONS' action request
        if (action == 'OPTIONS') {
            this._log(
                'OUTGOING',
                `OPTIONS >> [${identity}] >> [EDITOR] >> [${request.ip}] <> [${identifier}]`
            );
            return response.json({
                options: object.options,
            });
        }

        // Handle 'FILES' action request
        if (action == 'FILES') {
            // Convert LiveFile instances to a stringifiable object
            const live_files = object.directory.tree.files;
            const files = {};
            Object.keys(live_files).forEach((relative_path) => {
                const live_file = live_files[relative_path];
                files[relative_path] = live_file.etag;
            });

            this._log('OUTGOING', `FILES >> [${identity}] >> [EDITOR] >> [${identifier}]`);
            return response.json({
                files,
            });
        }

        // Handle 'READ' action request
        if (action == 'READ') {
            // Ensure LiveDirectory is ready before serving buffer
            await object.directory.ready();
            const body = await request.json();
            const file = object.directory.get(body.path);
            if (file) {
                this._log('OUTGOING', `READ >> [${identity}] >> [${body.path}] >> [${identifier}]`);
                return response.send(file.buffer);
            } else {
                return response.status(404).json({
                    code: 'FILE_NOT_FOUND',
                });
            }
        }

        // Handle 'CREATE_DIRECTORY' action request
        if (action == 'CREATE_DIRECTORY') {
            const body = await request.json();
            const relative_path = body.path;
            if (relative_path == undefined)
                return response.status(400).json({
                    code: 'NO_PATH_PROVIDED',
                });

            // Ensure directory doesn't already exist
            const system_path = object.path + relative_path;
            if (await accessible_path(system_path))
                return response.json({
                    code: 'ALREADY_EXISTS',
                });

            // Create directory
            await create_directory(system_path);
            this._log(
                'INCOMING',
                `CREATE_DIRECTORY [${identifier}] >> [${identity}] >> [${body.path}]`
            );
            return response.json({
                code: 'CREATED',
            });
        }

        // Handle 'WRITE' action request
        if (action == 'WRITE') {
            // Parse path from incoming request
            const relative_path = request.headers['relative-path'];
            const etag = request.headers['file-etag'];
            if (relative_path == undefined)
                return response.status(400).json({
                    code: 'NO_PATH_SPECIFIED',
                });

            // See if file already exists and the etag matches
            const incoming_file = object.directory.get(relative_path);
            if (incoming_file && incoming_file.etag === etag)
                return response.json({
                    code: 'NO_WRITE',
                    reason: 'ETAG_MATCH',
                });

            // Parse file buffer from incoming file content
            const buffer = await request.buffer();

            // Write file to source directory
            await write_file(object.path + relative_path, buffer);
            this._log('INCOMING', `WRITE [${identifier}] >> [${identity}] >> [${relative_path}]`);
            return response.json({
                code: 'WRITTEN',
            });
        }

        // Handle 'DELETE' action request
        if (action == 'DELETE') {
            // Parse path from incoming request
            const body = await request.json();
            const relative_path = body.path;
            if (typeof relative_path !== 'string')
                return response.status(400).json({
                    code: 'NO_PATH_SPECIFIED',
                });

            await delete_path(object.path + relative_path);
            this._log('INCOMING', `DELETE [${identifier}] >> [${identity}] >> [${relative_path}]`);
            return response.json({
                code: 'DELETED',
            });
        }

        // Return invalid action status code when it is not handled above
        response.status(404).json({
            code: 'INVALID_ACTION',
        });
    }

    /**
     * LiveDirectory constructor options
     *
     * @typedef {LiveDirectoryOptions}
     * @type {Object}
     * @property {Object} options
     * @property {String} options.path Path of the desired directory
     * @property {Object} options.keep Keep/Whitelist filter.
     * @property {Array} options.keep.names List of files/directories to keep/whitelist.
     * @property {Array} options.keep.extensions List of file extensions to keep/whitelist.
     * @property {Object} options.ignore Ignore/Blacklist filter
     * @property {Array} options.ignore.names List of files/directories to ignore/blacklist.
     * @property {Array} options.ignore.extensions List of file extensions to ignore/blacklist.
     */

    /**
     * Hosts a directory to make it available for cloning from a remote machine.
     *
     * @param {String} name Unique identifier/name of directory being hosted that clone instances can clone remotely.
     * @param {String} path Path of the directory you would like to host.
     * @param {LiveDirectoryOptions} options Options for hosting a directory.
     */
    async host(name, path, options = {}) {
        // Resolve the path to get absolute system path
        path = to_forward_slashes(Path.resolve(path));

        // Ensure this directory is not already being hosted
        if (this.#directories[name] || this.#directories[path])
            throw new Error(
                `Directory ${name} or ${path} is already being hosted. You cannot host duplicate directories.`
            );

        // Validate provided path to see if it is accessible
        if (!(await accessible_path(path)))
            throw new Error('No accessible directory found at specified path.');

        // Create new LiveDirectory instance
        options.path = path;
        this.#directories[path] = new LiveDirectory(options);
        this.#identities[name] = {
            path,
            directory: this.#directories[path],
            options: options,
        };
    }
}

module.exports = Source;
