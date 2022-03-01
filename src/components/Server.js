import Path from 'path';
import Stream from 'stream';
import EventEmitter from 'events';
import HyperExpress from 'hyper-express';
import DirectoryMap from './directory/DirectoryMap.js';
import DirectoryManager from './directory/DirectoryManager.js';

import { wrap_object, is_accessible_path, to_forward_slashes } from '../utils/operators.js';

export default class Server extends EventEmitter {
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

        // Bind the appropriate middlewares & communication routes
        this._bind_authentication_middleware();
        this._bind_http_route();
        this._bind_ws_route();

        // Listen on specified user port
        this.#server
            .listen(port)
            .then(() => this._log('STARTUP', `Server started on port ${port}`))
            .catch((error) => this.emit('error', error));
    }

    /**
     * Binds global authentication middleware to authenticate all incoming network requests.
     * @private
     */
    _bind_authentication_middleware() {
        // Bind the global middleware for request authentication
        const headers = this.#options.auth.headers;
        this.#server.use((request, response, next) => {
            // Ensure that the request has all of the required auth headers
            let is_authenticated = true;
            if (typeof headers == 'object')
                Object.keys(headers).forEach((key) => {
                    const value = headers[key];
                    if (request.headers[key] !== value) is_authenticated = false;
                });

            // Process request based on whether request is authenticated
            if (is_authenticated) {
                return next();
            } else {
                return response.status(403).json({
                    code: 'UNAUTHORIZED',
                    message: 'Please provide valid authentication headers.',
                });
            }
        });
    }

    /**
     * Binds the master HTTP route which will handle all incoming communications.
     * @private
     */
    _bind_http_route() {
        // Create the global catch-all HTTP route
        this.#server.any('/', async (request, response) => {
            // Destructure various path/query parameters
            let { host, uri } = request.query_parameters;

            // Decode the host/uri url encoded components
            host = decodeURIComponent(host);
            if (uri) uri = decodeURIComponent(uri);

            // Ensure that the incoming request maps to a valid host
            if (typeof host !== 'string' || this.#hosts[host] == undefined)
                return response.status(404).json({
                    code: 'INVALID_HOST',
                    message: `The specified host '${typeof host == 'string' ? host : ''}' is invalid.`,
                });

            // Return the schema of the map for this host if no uri is provided
            const { manager, map } = this.#hosts[host];
            if (uri === undefined)
                return response.json({
                    options: map.options,
                    schema: map.schema,
                });

            // If uri is provided, ensure it is a valid string
            if (typeof uri !== 'string' || uri.length == 0)
                return response.status(400).json({
                    code: 'INVALID_URI',
                    message: "Query parameter 'uri' must be a valid string.",
                });

            // Retrieve the associated map record for the provided uri
            const record = map.get(uri);
            if (record === undefined)
                return response.status(404).json({
                    code: 'NOT_FOUND',
                    message: 'No record exists for the specified uri.',
                });

            // Match the incoming request to one of the supported operations
            let body;
            let operation;
            switch (request.method) {
                case 'GET':
                    // Retrieve a readable stream for the specified uri
                    operation = manager.read(uri, true);
                    break;
                case 'PUT':
                    // Parse the JSON body to analyze the uri record
                    // Records with only two properties are directories
                    body = await request.json();
                    operation = manager.create(uri, body.length === 2);
                    break;
                case 'POST':
                    operation = manager.write(uri, request.stream);
                    break;
                case 'DELETE':
                    operation = manager.delete(uri);
                    break;
            }

            // Determine if we were able to successfully map the request to an operation
            if (operation) {
                // Safely retrieve the output from the operation by awaiting if it is a promise
                let output;
                try {
                    output = operation instanceof Promise ? await operation : operation;
                } catch (error) {
                    // Return the request with the error message
                    return response.status(500).json({
                        code: 'INTERNAL_ERROR',
                        message: 'An uncaught error DirectoryManager error occured.',
                        error: error?.message,
                    });
                }

                // If the output of the operation is a readable stream, pipe it as the response
                if (output instanceof Stream.Readable) {
                    return output.pipe(response.writable);
                } else {
                    // Send a 'SUCCESS' code response with any output as that data
                    return response.json({
                        code: 'SUCCESS',
                        data: output,
                    });
                }
            } else {
                // The request was an unsupported HTTP method
                return response.status(405).json({
                    code: 'UNSUPPORTED_METHOD',
                    message: `The HTTP method '${request.method}' is not supported.`,
                });
            }
        });
    }

    /**
     * Binds the master WebSocket route which will handle all incoming websocket communications.
     * @private
     */
    _bind_ws_route() {
        // Create the global catch-all WebSocket route
        // We do not need to authenticate this endpoint as the global middleware will authenticate
        this.#server.ws('/', (ws) => {
            // Subscribe the connection to 'map_events' topic
            ws.subscribe('map_events');
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
        const manager = new DirectoryManager(map);

        // Bind a error handler to pass through any errors
        map.on('error', (error) => this.emit('error', error));

        // Wait for the DirectoryMap to be ready
        await map.ready();

        // Create a record for this host instance
        this.#hosts[name] = {
            path,
            map,
            manager,
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
