import Path from 'path';
import Stream from 'stream';
import EventEmitter from 'events';
import HyperExpress from 'hyper-express';
import DirectoryMap from './directory/DirectoryMap.js';
import DirectoryManager from './directory/DirectoryManager.js';

import {
    wrap_object,
    is_accessible_path,
    to_forward_slashes,
    safe_json_parse,
    ascii_to_hex,
} from '../utils/operators.js';

export default class Server extends EventEmitter {
    #server;
    #hosts = {};
    #options = {
        port: 8080,
        auth: '',
        ssl: {
            key: '',
            cert: '',
            passphrase: '',
            dh_params: '',
            prefer_low_memory_usage: false,
        },
        limits: {
            max_body_length: 1024 * 1024 * 100, // 100MB
            fast_buffers: true,
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
        // Spread constructor options for HyperExpress Server instance
        const { port, ssl, limits } = this.#options;
        const { key, cert, passphrase, dh_params, prefer_low_memory_usage } = ssl;

        // Create a new HyperExpress.Server instance
        if (key && cert) {
            this.#server = new HyperExpress.Server({
                key_file_name: key,
                cert_file_name: cert,
                passphrase: passphrase,
                dh_params_file_name: dh_params,
                ssl_prefer_low_memory_usage: prefer_low_memory_usage,
                ...limits,
            });
        } else {
            this.#server = new HyperExpress.Server({
                ...limits,
            });
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
        // Bind the global middleware for network authentication
        const auth_key = this.#options.auth;
        this.#server.use((request, response, next) => {
            // Retrieve the incoming request's authorization key
            const incoming_key = request.headers['x-auth-key'] || request.query_parameters['auth_key'] || '';

            // Process request based on whether request is authenticated
            if (auth_key === incoming_key) {
                return next();
            } else {
                return response.status(403).json({
                    code: 'UNAUTHORIZED',
                    message: 'Please provide a valid authentication key.',
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
            let { uri, host } = request.query_parameters;
            const content_length = +request.headers['content-length'] || 0;

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
            if (uri === undefined) {
                this._log('SCHEMA', `'${host}' - ${request.ip}`);
                return response.json({
                    options: map.options,
                    schema: map.schema,
                });
            }

            // If uri is provided, ensure it is a valid string
            if (typeof uri !== 'string' || uri.length == 0)
                return response.status(400).json({
                    code: 'INVALID_URI',
                    message: "Query parameter 'uri' must be a valid string.",
                });

            // Retrieve the Directory Map record for this uri
            const record = map.get(uri);

            // Match the incoming request to one of the supported operations
            let body;
            let operation;
            let descriptor;
            let is_directory = false;
            switch (request.method) {
                case 'GET':
                    // Ensure that the uri has a valid record in our map
                    if (record === undefined)
                        return response.status(404).json({
                            code: 'NOT_FOUND',
                            message: 'No record exists for the specified uri.',
                        });

                    // Respond with the record's data as a stream or empty
                    // Include the md5-hash of the record's content if it exists
                    descriptor = 'UPLOAD';
                    operation = record.stats.size > 0 ? manager.read(uri, true) : '';

                    // Include the MD5 hash of the record's content if it exists so client can verify
                    response.header('md5-hash', record.stats.md5);
                    break;
                case 'PUT':
                    // Parse the JSON body to analyze the uri record
                    // Records with only two properties are directories
                    descriptor = 'CREATE';
                    body = await request.json();
                    is_directory = body.length === 2;
                    operation = manager.create(uri, is_directory);
                    break;
                case 'POST':
                    descriptor = 'DOWNLOAD';

                    // Do not write to the file system if the incoming md5 is the same as the local md5
                    const incoming_md5 = request.headers['md5-hash'];
                    const local_md5 = record?.stats?.md5;
                    if (incoming_md5 === local_md5) {
                        // Mark the operation as complete with an empty response
                        operation = '';
                        break;
                    }

                    // Consume the incoming stream if we have some content else empty the file
                    operation = manager.indirect_write(uri, content_length == 0 ? '' : request, incoming_md5);
                    break;
                case 'DELETE':
                    descriptor = 'DELETE';
                    operation = manager.delete(uri);
                    break;
            }

            // Determine if we were able to successfully map the request to an operation
            if (operation !== undefined) {
                // Safely retrieve the output from the operation by awaiting if it is a promise
                let output;
                try {
                    // Safely derive an output from the operation
                    const start_time = Date.now();
                    output = operation instanceof Promise ? await operation : operation;

                    // Log the operation if it has a mutation descriptor
                    const execution_time = Date.now() - start_time;
                    this._log(
                        descriptor,
                        `${request.ip} - '${host}' - ${uri}${execution_time > 0 ? ` - ${execution_time}ms` : ''}`
                    );
                } catch (error) {
                    // Return a 409 HTTP status code to signify that the operation failed
                    if (error.message === 'ERR_FILE_WRITE_INTEGRITY_CHECK_FAILED')
                        return response.status(409).json({
                            code: 'DELIVERY_FAILED',
                            message: 'The file integrity check failed. Please re-upload the file.',
                        });

                    // Return a 500 HTTP status code to signify a server error
                    return response.status(500).json({
                        code: 'SERVER_ERROR',
                        message: 'An uncaught error DirectoryManager error occured.',
                        error: error?.message,
                    });
                }

                // GET requests are only used to consume data thus we must only send output
                if (request.method === 'GET') {
                    // If the output of the operation is a readable stream, pipe it as the response
                    if (output instanceof Stream.Readable) {
                        return response.stream(output, record.stats.size);
                    } else {
                        return response.send(output);
                    }
                } else {
                    // Send a 'SUCCESS' code response with any output as that data parameter
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
        const reference = this;
        this.#server.ws('/', (ws) => {
            reference._log('WEBSOCKET', `CONNECTED - ${ws.ip}`);

            // Bind a 'message' handler to handle incoming messages
            ws.on('message', (message) => {
                // Safely parse the incoming message as JSON
                message = safe_json_parse(message);
                if (message)
                    switch (message.command) {
                        case 'SUBSCRIBE':
                            // Subscribe to the specified host in hexadecimal format
                            const topic = `events/${ascii_to_hex(message.host)}`;
                            if (!ws.is_subscribed(topic)) {
                                reference._log('WEBSOCKET', `SUBSCRIBED - ${ws.ip} - '${message.host}'`);
                                ws.subscribe(topic);
                            }
                            break;
                    }
            });

            // Bind a 'close' handler to handle disconnections
            ws.on('close', () => {
                reference._log('WEBSOCKET', `DISCONNECTED - ${ws.ip}`);
            });
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

        // Initialize the file size limit if user has not provided one
        const server_max_length = this.#options.limits.max_body_length;
        if (options?.limits?.max_file_size == undefined)
            options.limits = {
                max_file_size: server_max_length,
            };

        // Ensure the provided or specified max_file_size limit does not exceed the server max_body_length limit
        if ((options.limits.max_file_size || 0) > server_max_length)
            throw new Error(
                "DirectorySync.Server.host(name, path, options) -> The provided max_file_size limit exceeds the server's max_body_length."
            );

        // Create a new DirectoryMap instance for this host
        options.path = path;
        const map = new DirectoryMap(options);
        const manager = new DirectoryManager(map, false);

        // Bind a 'error' handler to pass through any errors
        map.on('error', (error) => this.emit('error', error));

        // Bind a 'file_size_limit' handler to log the event
        map.on('file_size_limit', (uri, object) =>
            this._log(
                'SIZE_LIMIT_REACHED',
                `${uri} - SIZE_${object.stats.size}_BYTES > LIMIT_${map.options.limits.max_file_size}_BYTES`
            )
        );

        // Wait for the DirectoryMap to be ready
        await map.ready();

        // Bind mutation emitters for this host
        this._bind_mutation_emitters(name, map);

        // Create a record for this host instance
        this.#hosts[name] = {
            path,
            map,
            manager,
        };
    }

    /**
     * Publishes a mutation event to all websocket consumers for the specified host identifier.
     *
     * @param {String} host
     * @param {String} uri
     * @param {('CREATE'|'MODIFIED'|'DELETE')} type
     * @param {Boolean} is_directory
     */
    _publish_mutation(host, uri, type, is_directory = true) {
        let md5 = '';
        if (type !== 'DELETE') {
            // Attempt to destructure and retrieve md5 from the uri record
            const { map } = this.#hosts[host];
            const record = map.get(uri);
            if (record) md5 = record.stats.md5 || '';
        }

        // Emit a 'mutation' event with the provided type, uri, and is_directory
        const identifier = ascii_to_hex(host);
        this.#server.publish(
            `events/${identifier}`,
            JSON.stringify({
                command: 'MUTATION',
                host,
                uri,
                md5,
                type,
                is_directory,
            })
        );
    }

    /**
     * Binds handlers to the host map for emitting mutations.
     *
     * @private
     * @param {String} host
     * @param {DirectoryMap} map
     */
    _bind_mutation_emitters(host, map) {
        // Bind a 'directory_create' handler to publish mutations
        map.on('directory_create', (uri) => this._publish_mutation(host, uri, 'CREATE', true));

        // Bind a 'directory_delete' handler to publish mutations
        map.on('directory_delete', (uri) => this._publish_mutation(host, uri, 'DELETE', true));

        // Bind a 'file_create' handler to publish mutations
        map.on('file_create', (uri) => this._publish_mutation(host, uri, 'CREATE', false));

        // Bind a 'file_change' handler to publish mutations
        map.on('file_change', (uri) => this._publish_mutation(host, uri, 'MODIFIED', false));

        // Bind a 'file_delete' handler to publish mutations
        map.on('file_delete', (uri) => this._publish_mutation(host, uri, 'DELETE', false));
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
