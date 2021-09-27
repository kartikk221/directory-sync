const Path = require('path');
const JustQueue = require('just-queue');
const EventEmitter = require('events');
const LiveDirectory = require('live-directory');
const fetch = require('fetch-easy');
const {
    wrap_object,
    async_for_each,
    accessible_path,
    create_directory,
    write_file,
    to_forward_slashes,
} = require('../shared/operators');

class Editor extends EventEmitter {
    #base;
    #queue;
    #directory;
    #created_at;
    #options = {
        path: '',
        identity: '',
        host: '',
        port: '',
        ssl: false,
        headers: {},
    };

    /**
     * Editor constructor options.
     *
     * @param {Object} options
     * @param {String} options.path Local path to which remote directory to should be cloned and synced.
     * @param {String} options.identity Identity string of the directory to clone
     * @param {String} options.host Host of the source server
     * @param {String} options.port Port of the source server
     * @param {Boolean} options.ssl Whether source server is serving over SSL
     * @param {Object} options.headers Headers to send on each request. Useful for authenticating on source server.
     */
    constructor(options) {
        super();

        // Wrap default options object with provided options
        if (options === null || typeof options !== 'object')
            throw new Error('DirectorySync.Host.options must be an object');

        // Resolve user provided path to an absolute path
        options.path = to_forward_slashes(Path.resolve(options.path));
        wrap_object(this.#options, options);

        // Determine base URL for source
        const { host, port, ssl } = this.#options;
        this.#base = `http${ssl ? 's' : ''}://${host}:${port}`;

        // Initialize queue for instance
        this.#queue = new JustQueue({
            max_concurrent: 1,
        });

        // Initialize LiveDirectory instance for specified path
        this.#created_at = Date.now();
        this._initiate_directory();
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
     * Makes fetch request to source with provided options.
     *
     * @private
     * @param {String} identity
     * @param {String} action
     * @param {Object} options
     */
    async _request(action, options = {}) {
        // Queue request if it has not been queued yet to prevent multiple concurrent filesystem operations
        if (options.queue !== false && options._queued == undefined) {
            options._queued = true;
            return await this.#queue.queue(() => this._request(action, options));
        }

        // Inject source options into fetch options object
        options.method = 'POST';
        if (options.headers) {
            options.headers = { ...options.headers, ...this.#options.headers };
        } else {
            options.headers = this.#options.headers;
        }

        // Perform fetch request
        const response = await fetch(`${this.#base}/${this.#options.identity}/${action}`, options);

        // throw the response if status code is not a 200 status code
        if (response.status !== 200) throw response;

        // Return the raw response if parsing is not requested
        if (options.parse === false) return response;

        // Parse status and body and return
        const status = response.status;
        const body = await response.json();
        return {
            status,
            body,
        };
    }

    /**
     * Initiates LiveDirectory instance for local path
     * @private
     */
    async _initiate_directory() {
        // Ensure local root directory actually exists
        if (!(await accessible_path(this.#options.path))) {
            await create_directory(this.#options.path);
            this._log('CREATE', 'Created Local Root Directory');
        }

        // Synchronize LiveDirectory options with remote source server
        this._log('STARTUP', 'Synchronizing Directory Options With Source...');
        let options;
        try {
            // Synchronize LiveDirectory options with source
            const { body } = await this._request('OPTIONS');
            if (body.options == undefined)
                throw new Error('No Options Received. Could be due to bad authentication.');
            options = body.options;
        } catch (response) {
            const error = response.json ? await response.json() : response;
            this._log(
                'ERROR',
                'Failed To Synchronize Directory Options With Source! Aborted Startup.'
            );
            return this.emit('error', error);
        }

        // Create LiveDirectory instance with retrieved options
        this._log('STARTUP', 'Initializing Local Root Directory...');
        options.path = this.#options.path;
        this.#directory = new LiveDirectory(options);
        await this.#directory.ready();

        // Perform initial tree synchronization with remote
        this._initial_sync();
    }

    /**
     * Synchronizes local directory with remote directory based on tree differences
     * @private
     */
    async _initial_sync() {
        // Get remote files from source and destructure
        this._log('STARTUP', 'Synchronizing Local Directory With Remote...');
        const { body } = await this._request('FILES');
        const { files } = body;

        // Throw error on not being able to retrieve files
        if (typeof files !== 'object' || files == null) {
            this._log(
                'ERROR',
                'Failed To Perform Initial Directory Synchronization With Remote! Aborted Startup.'
            );
            return this.emit('error', new Error('No Files Received On Initial Sync'));
        }

        // Compare remote directories against local and synchronize differences
        await this._compare_directories(files);

        // Compare files against local and synchronize differences
        await this._compare_files(files);

        // Bind mutation handlers to begin watching local directory
        this._bind_mutation_handlers();
        this._log('READY', `Finished Startup In ${Date.now() - this.#created_at}ms`);
    }

    /**
     * Compares provided object of remote files' paths to
     * fill in missing directories so no write errors occur
     * due to missing heirarchy directories.
     *
     * @private
     * @param {Object} files
     */
    async _compare_directories(files) {
        const relative_paths = Object.keys(files);
        const local_root = this.#options.path;
        if (relative_paths.length > 0) {
            // Parse relative paths into chunks with just their paths
            const parsed = relative_paths
                .map((path) => {
                    const chunks = path.split('/');
                    const relative = chunks.splice(1, chunks.length - 2);
                    return {
                        relative: relative.join('/'),
                        depth: relative.length,
                    };
                })
                .filter((object) => object.depth > 0);

            // Sort by increasing depth
            parsed.sort((a, b) => a.depth - b.depth);

            // Ensure all parsed directories exist and create when they do not
            await async_for_each(parsed, async (directory, next) => {
                const path = local_root + '/' + directory.relative;
                if (!(await accessible_path(path))) await create_directory(path);
                next();
            });
        }
    }

    /**
     * Compares provided object of remote files against local files
     * Downloads files that are missing locally
     * Uploading files that have a different etag then remote.
     *
     * @private
     * @param {Object} files
     */
    async _compare_files(files) {
        // Compare remote files against local files
        const reference = this;
        const local_directory = this.#directory;
        const relative_paths = Object.keys(files);
        if (relative_paths.length > 0)
            await async_for_each(relative_paths, async (relative_path, next) => {
                // Download and initialize file in local directory if it does not exist locally
                const local_file = local_directory.get(relative_path);
                if (local_file) {
                    // Compare etags of local file against remote file and upload on mismatch
                    const etag = files[relative_path];
                    if (etag !== local_file.etag) await reference._upload_file(relative_path);
                } else {
                    // Download file from remote source server
                    await reference._download_file(relative_path);

                    // Lock this file for a single file_reload event
                    // Local LiveDirectory needs time to catch up
                    reference.#locks[relative_path] = true;
                }

                // Move to next file
                next();
            });
    }

    /**
     * Instructs remote server to create directory at specified file.
     *
     * @private
     * @param {String} relative_path
     */
    async _create_directory(relative_path) {
        // Parse file name to generate description
        const chunks = relative_path.split('/');
        const name = chunks[chunks.length - 1];
        const description = `[${name}] >> [${relative_path}] >> [REMOTE]`;

        // Download file from remote source as it doesn't exist locally
        try {
            const start_time = Date.now();
            this._log('PENDING', `CREATE_DIRECTORY ${description}`);
            await this._request('CREATE_DIRECTORY', {
                body: JSON.stringify({
                    path: relative_path,
                }),
            });
            this._log(
                'COMPLETE',
                `CREATE_DIRECTORY ${description} >> [${Date.now() - start_time}ms]`
            );
        } catch (error) {
            this._log('ERROR', `CREATE_DIRECTORY_FAIL ${description}`);
            this.emit('error', error);
        }
    }

    /**
     * Uploads local file to remote source.
     *
     * @private
     * @param {String} relative_path
     */
    async _upload_file(relative_path) {
        const file = this.#directory.get(relative_path);
        if (file == undefined)
            return this._log(
                'ERROR',
                `Attempted To Upload Non-Existent Local File @ ${relative_path}`
            );

        // Initiate file upload to remote
        const start_time = Date.now();
        const description = `[${file.name}] >> [${relative_path}] >> [REMOTE]`;
        this._log('PENDING', `UPLOAD ${description}`);
        const { status, body } = await this._request('WRITE', {
            headers: {
                'relative-path': relative_path,
                'file-etag': file.etag,
            },
            body: file.buffer,
        });

        // Log error on a non 200 response
        if (status !== 200) {
            this._log('ERROR', `UPLOAD_FAIL ${description}`);
            return this.emit('error', body);
        }

        // Log completion with timestamp in milliseconds
        this._log('COMPLETE', `UPLOAD ${description} >> [${Date.now() - start_time}ms]`);
    }

    /**
     * Downloads and writes remote file to local directory.
     *
     * @private
     * @param {String} relative_path
     */
    async _download_file(relative_path) {
        // Parse file name to generate description
        const chunks = relative_path.split('/');
        const name = chunks[chunks.length - 1];
        const description = `[${name}] >> [REMOTE] >> [${relative_path}]`;

        // Download file from remote source as it doesn't exist locally
        try {
            const start_time = Date.now();
            this._log('PENDING', `DOWNLOAD ${description}`);
            const response = await this._request('READ', {
                parse: false,
                body: JSON.stringify({
                    path: relative_path,
                }),
            });

            // Ensure remote does not respond with 404
            if (response.status == 404) throw new Error('File does not exist at remote.');

            // Parse downloaded file as a buffer
            const buffer = await response.buffer();

            // Write downloaded file buffer locally to its location
            await write_file(this.#options.path + relative_path, buffer);
            this._log('COMPLETE', `DOWNLOAD ${description} >> [${Date.now() - start_time}ms]`);
        } catch (error) {
            this._log('ERROR', `DOWNLOAD_FAIL ${description}`);
            this.emit('error', error);
        }
    }

    /**
     * Instructs remote server to delete files/directories at specified path.
     *
     * @private
     * @param {String} relative_path
     */
    async _delete_path(relative_path) {
        // Parse file name to generate description
        const chunks = relative_path.split('/');
        const name = chunks[chunks.length - 1];
        const description = `[${name}] >> [${relative_path}] >> [REMOTE]`;

        // Download file from remote source as it doesn't exist locally
        try {
            const start_time = Date.now();
            this._log('PENDING', `DELETE ${description}`);
            await this._request('DELETE', {
                body: JSON.stringify({
                    path: relative_path,
                }),
            });
            this._log('COMPLETE', `DELETE ${description} >> [${Date.now() - start_time}ms]`);
        } catch (error) {
            this._log('ERROR', `DELETE_FAIL ${description}`);
            this.emit('error', error);
        }
    }

    /**
     * Returns relative path from system path based on instance root.
     *
     * @private
     * @param {String} path
     * @returns {String}
     */
    _relative_path(path) {
        let relative = path.replace(this.#options.path, '');
        if (!relative.startsWith('/')) relative = '/' + relative;
        return relative;
    }

    #locks = {};

    /**
     * @private
     * Binds handlers for local directory mutations to send changes to source.
     */
    _bind_mutation_handlers() {
        const reference = this;

        // Upload local file to remote on file_change
        this.#directory.on('file_reload', (file) => {
            const relative_path = reference._relative_path(file.path);
            const is_locked = reference.#locks[relative_path];

            // If file is locked, then release the lock, else upload it.
            if (is_locked === true) {
                delete reference.#locks[relative_path];
            } else {
                reference._upload_file(relative_path);
            }
        });

        // Delete remote path on file_destroy
        this.#directory.on('file_destroy', (file) => {
            const relative_path = reference._relative_path(file.path);
            reference._delete_path(relative_path);
        });

        // Destroy remote directory on directory_destroy
        this.#directory.on('directory_create', (path) => {
            const relative_path = reference._relative_path(path);
            reference._create_directory(relative_path);
        });

        // Destroy remote directory on directory_destroy
        this.#directory.on('directory_destroy', (path) => {
            const relative_path = reference._relative_path(path);
            reference._delete_path(relative_path);
        });
    }
}

module.exports = Editor;
