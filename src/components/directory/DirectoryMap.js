const Path = require('path');
const Chokidar = require('chokidar');
const EventEmitter = require('events');
const FileSystem = require('fs');
const { wrap_object, match_extension, to_forward_slashes, is_accessible_path } = require('../../utils/operators.js');

/**
 * @typedef {Object} FilteringObject
 * @property {Array<string>} files - The file names to filter (case sensitive). Examples: ['.env', 'secrets.json']
 * @property {Array<string>} directories - The directory names to filter (case sensitive). Examples: ['node_modules']
 * @property {Array<string>} extensions - The file/directory extensions to filter (case sensitive). Examples: `['.js', '.plugin.js', '.json']`.
 */

/**
 * @typedef {function(String, FileSystem.Stats): Boolean} FilteringFunction
 */

/**
 * @typedef {Object} DirectoryMapOptions
 * @property {String} path - The root path of the directory tree.
 * @property {Object} filters - The detection filters to apply to the directory tree.
 * @property {FilteringObject|FilteringFunction} filters.keep - Only files satisfying these filter(s) will be loaded into the Directory Tree.
 * @property {FilteringObject|FilteringFunction} filters.ignore - Files statisfying these filter(s) will NOT be loaded into the Directory Tree.
 * @property {Chokidar.WatchOptions} watcher - The watcher options to use when watching the directory tree.
 */

/**
 * @typedef {Object} MapRecord
 * @property {String} path
 * @property {FileSystem.Stats} stats
 */

class DirectoryMap extends EventEmitter {
    #path;
    #watcher;
    #files = {};
    #directories = {};
    #options = {
        path: '',
        filters: {
            keep: {},
            ignore: {},
        },
        watcher: {
            awaitWriteFinish: {
                pollInterval: 100,
                stabilityThreshold: 500,
            },
        },
    };

    /**
     * Initializes a new DirectoryMap instance.
     * @param {DirectoryMapOptions} options
     */
    constructor(options) {
        // Ensure options is valid object
        if (options === null || typeof options !== 'object')
            throw new Error('new DirectoryMap(options) -> options is not an object.');

        // Ensure we received a valid options.path
        if (typeof options.path !== 'string')
            throw new Error('new DirectoryMap(options.path) -> path must be a String.');

        // Initialize the EventEmitter class
        super();

        // Store the provided options locally for future access
        options.path = to_forward_slashes(Path.resolve(options.path));
        wrap_object(this.#options, options);
        this.#path = options.path;

        // Parse the "keep" and "ignore" filter functions into usable functions
        const reference = this;
        const root_directory = this.#path.split('/').slice(-1)[0];
        const { keep, ignore } = this.#options.filters;
        [keep, ignore].forEach((filter, index) => {
            // Index 0 is "keep" and index 1 is "ignore"
            const FILTER_TYPE = index === 0 ? 'keep' : 'ignore';
            if (typeof filter == 'function') {
                // If the filter is a function, use it as is
                reference.#options.filters[FILTER_TYPE] = filter;
            } else if (filter && typeof filter == 'object') {
                // Destructure the names and extensions from the filter
                const { files, directories, extensions } = filter;
                const has_files = Array.isArray(files) && files.length > 0;
                const has_directories = Array.isArray(directories) && directories.length > 0;
                const has_extensions = Array.isArray(extensions) && extensions.length > 0;

                // If the filter has names or extensions, use a function to filter the DirectoryMap
                if (has_files || has_directories || has_extensions)
                    reference.#options.filters[FILTER_TYPE] = (path, stats, strict) => {
                        // Retrieve the file's name by getting the last slash split chunk
                        const chunks = path.split('/');
                        const name = chunks[chunks.length - 1];
                        const is_root = path === `/${name}`;
                        const is_directory = stats.isDirectory();

                        // Determine if the file is a directory
                        if (is_directory) {
                            // Only perform below checks if we have some directories to check
                            if (has_directories) {
                                // If this directory's name matches one of the directory names, return true
                                if (directories.includes(name)) return true;

                                // If strict mode is disabled, check for parent directory matches
                                if (!strict && chunks.find((parent) => directories.includes(parent))) return true;
                            }
                        } else {
                            // If this file's name matches one of the names, return true
                            if (has_files && files.includes(name)) return true;

                            // If this is a file and its extension matches one of the extensions, return true
                            if (has_extensions && extensions.find((ext) => match_extension(name, ext))) return true;

                            // If this is a root file and we have directories to check for this filter
                            // We must handle the scenario where root files are ONLY allowed if the root directory is allowed
                            if (is_root && has_directories) return directories.includes(root_directory);
                        }

                        // If strict mode is enabled, always return false as this is a strict filter
                        // If strict mode is disabled, return true ONLY if none of the above conditions were checked for
                        return strict ? false : is_directory ? !has_directories : !has_files && !has_extensions;
                    };
            }
        });

        // Initialize the chokidar watcher instance
        this._initialize_watcher().catch((error) => this.emit('error', error));
    }

    #ready_promise;
    #ready_resolve;

    /**
     * Returns a Promise which is resolved once this DirecotryTree instance is ready to be used.
     * @returns {Promise<void>}
     */
    ready() {
        // Return the cached promise if available
        if (this.#ready_promise) return this.#ready_promise;

        // Create a new promise for consuming the 'ready' event from watcher
        this.#ready_promise = new Promise((resolve) => (this.#ready_resolve = resolve));

        // Return this promise for the current user
        return this.#ready_promise;
    }

    /**
     * Initializes the underlying watcher instance that will power this directory tree.
     * @private
     */
    async _initialize_watcher() {
        // Retrieve the root path from user options
        const { path, watcher } = this.#options;

        // Ensure the provided root path is accessible
        if (!(await is_accessible_path(path)))
            throw new Error(`new DirectoryMap(options.path) -> Unable to access the provided path: ${path}`);

        // Inject the top level filter callback into the watcher options
        const reference = this;
        const { keep, ignore } = this.#options.filters;
        watcher.ignored = (path, stats) => {
            // If this execution does not have stats avaialble, ignore it
            if (stats === undefined) return false;

            // Always allow the root path to prevent premature traversal halt
            if (path === reference.#path) return false;

            // Extrapolate the relative path for filtering this file/directory
            const relative = reference._relative_path(path);

            // Assert the "ignore" filter as strict if one is available
            // The "ignore" filter is applied first as it is more restrictive
            if (typeof ignore == 'function' && ignore(relative, stats, true)) return true;

            // Assert the "keep" filter as non-strict if one is available
            if (typeof keep == 'function' && !keep(relative, stats, false)) return true;

            // If this candidate passes above filters, then it is good to be tracked
            return false;
        };

        // Initialize the chokidar watcher instance for this root path
        this.#watcher = Chokidar.watch(path, watcher);

        // Bind appropriate handlers to consume the watcher events
        this.#watcher.on('addDir', (path, stats) => this._on_directory_create(path, stats));
        this.#watcher.on('unlinkDir', (path) => this._on_directory_delete(path));
        this.#watcher.on('add', (path, stats) => this._on_file_add(path, stats));
        this.#watcher.on('unlink', (path) => this._on_file_delete(path));
        this.#watcher.on('change', (path, stats) => this._on_file_change(path, stats));
        this.#watcher.on('ready', () => (this.#ready_resolve ? this.#ready_resolve() : undefined));

        // Bind a global close handler to close the chokidar instance
        // This will prevent the watcher from hanging on to the process
        ['exit', 'SIGINT', 'SIGUSR1', 'SIGUSR2', 'SIGTERM', 'uncaughtException'].forEach((type) =>
            process.once(type, () => reference.#watcher.close())
        );
    }

    /**
     * Returns the relative path to the root of the directory tree.
     *
     * @private
     * @param {String} path
     * @returns {String}
     */
    _relative_path(path) {
        // Retrieve the relative path by removing the root path from the provided path
        return to_forward_slashes(path).replace(this.#options.path, '');
    }

    /**
     * @typedef {Object} FilteredStats
     * @property {Number} size - The size of the file in bytes.
     * @property {Number} created_at - The time the file was created in milliseconds since epoch.
     * @property {Number} modified_at - The time the file was last modified in milliseconds since epoch.
     */

    /**
     * Returns a filtered object with only important FileSystem.stats properties.
     *
     * @private
     * @param {FileSystem.Stats} stats
     * @returns {FilteredStats}
     */
    _filtered_stats(stats) {
        return {
            size: stats.size,
            created_at: Math.round(stats.birthtimeMs),
            modified_at: Math.round(stats.mtimeMs),
        };
    }

    /**
     * @typedef {Object} ObjectStats
     * @property {String} uri - The relative path/uri of the object file/directory.
     * @property {String} path - The absolute path to the object file/directory.
     * @property {FilteredStats} stats - The filtered stats of the object file/directory.
     */

    /**
     * Returns a formed object state for the provided file/directory path.
     *
     * @private
     * @param {String} path
     * @param {FileSystem.Stats} stats
     * @returns {ObjectStats}
     */
    _object_stats(path, stats) {
        // Retrieve the relative path to the directory
        const relative_path = this._relative_path(path);

        // Ignore the root directory from being stored in directories map
        if (relative_path.length == 0) return;

        // Return the formated object stats
        return {
            uri: relative_path,
            path: to_forward_slashes(path),
            stats: this._filtered_stats(stats),
        };
    }

    /**
     * Handles the watcher directory create event.
     *
     * @private
     * @param {String} path
     * @param {FileSystem.Stats} stats
     */
    _on_directory_create(path, stats) {
        // Retrieve the relative path to the directory
        const object = this._object_stats(path, stats);
        if (object) {
            // Store the object by the relative path aka uri
            this.#directories[object.uri] = object;

            // Emit the directory create event
            this.emit('directory_create', object.uri, object);
        }
    }

    /**
     * Handles the watcher directory delete event.
     *
     * @private
     * @param {String} path
     */
    _on_directory_delete(path) {
        // Retrieve the relative path to the directory
        const relative_path = this._relative_path(path);

        // Delete the directory's record from the directory map
        delete this.#directories[relative_path];

        // Emit the directory delete event for a higher consumer
        this.emit('directory_delete', relative_path);
    }

    /**
     * Handles the watcher file add event.
     *
     * @private
     * @param {String} path
     * @param {FileSystem.Stats} stats
     * @param {Boolean} is_change
     */
    _on_file_add(path, stats, is_change = false) {
        // Retrieve the relative path to the directory
        const object = this._object_stats(path, stats);
        if (object) {
            // Store the object by the relative path aka uri
            this.#files[object.uri] = object;

            // Emit the directory create event
            this.emit(is_change ? 'file_change' : 'file_add', object.uri, object);
        }
    }

    /**
     * Handles the watcher file delete event.
     *
     * @private
     * @param {String} path
     */
    _on_file_delete(path) {
        // Retrieve the relative path to the directory
        const relative_path = this._relative_path(path);

        // Delete the file's record from the directory map
        delete this.#files[relative_path];

        // Emit the file delete event for a higher consumer
        this.emit('file_delete', relative_path);
    }

    /**
     * Handles the watcher file change event.
     *
     * @private
     * @param {String} path
     * @param {FileSystem.Stats} stats
     */
    _on_file_change(path, stats) {
        // Pass through this event to the file add event but as a change event
        this._on_file_add(path, stats, true);
    }

    /* DirectoryMap Getters */

    /**
     * Returns the root path of this DirectoryMap.
     * @returns {String}
     */
    get path() {
        return this.#path;
    }

    /**
     * Returns the chokidar watcher instance.
     * @returns {Chokidar}
     */
    get watcher() {
        return this.#watcher;
    }

    /**
     * Returns the options used to initialize this DirectoryMap.
     * @returns {DirectoryMapOptions}
     */
    get options() {
        return this.#options;
    }

    /**
     * Returns all the directories in this DirectoryMap.
     * @returns {Object<string, MapRecord>}
     */
    get directories() {
        return this.#directories;
    }

    /**
     * Returns all the files in this DirectoryMap.
     * @returns {Object<string, MapRecord>}
     */
    get files() {
        return this.#files;
    }
}

module.exports = DirectoryMap;