import Path from 'path';
import Chokidar from 'chokidar';
import EventEmitter from 'events';
import FileSystem from 'fs';
import {
    wrap_object,
    match_extension,
    to_path_uri,
    to_forward_slashes,
    is_accessible_path,
    generate_md5_hash,
} from '../../utils/operators.js';

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
 * @property {Object} limits - The limit constants for this Directory Map.
 * @property {Number} limits.max_file_size - The maximum file size in bytes.
 * @property {Chokidar.WatchOptions} watcher - The watcher options to use when watching the directory tree.
 */

/**
 * @typedef {Object} MapRecord
 * @property {String} path
 * @property {FileSystem.Stats} stats
 */

export default class DirectoryMap extends EventEmitter {
    #path;
    #watcher;
    #cleanup;
    #files = {};
    #directories = {};
    #supressions = {};
    #destroyed = false;
    #options = {
        path: '',
        filters: {
            keep: {},
            ignore: {},
        },
        watcher: {
            alwaysStat: true,
            usePolling: false,
            awaitWriteFinish: {
                pollInterval: 100,
                stabilityThreshold: 500,
            },
        },
        limits: {
            max_file_size: 1024 * 1024 * 100,
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
        delete this.#options.path;

        // Parse the "keep" and "ignore" filter functions into usable functions
        const reference = this;
        const { keep, ignore } = this.#options.filters;
        [keep, ignore].forEach((filter, index) => {
            // Index 0 is "keep" and index 1 is "ignore"
            // We store the parsed filter functions with "_" prefix to sginify that they are not user-provided
            const FILTER_TYPE = index === 0 ? '_keep' : '_ignore';
            if (typeof filter == 'function') {
                // If the filter is a function, use it as is
                reference.#options.filters[FILTER_TYPE] = filter;
            } else if (filter && typeof filter == 'object') {
                // Destructure the names and extensions from the filter
                const { files, directories, extensions } = filter;
                const has_files = Array.isArray(files) && files.length > 0;
                const has_directories = Array.isArray(directories) && directories.length > 0;
                const has_extensions = Array.isArray(extensions) && extensions.length > 0;

                // Convert files, directories, and extensions to uris
                const file_uris = has_files ? files.map((file) => to_path_uri(file)) : [];
                const directory_uris = has_directories ? directories.map((directory) => to_path_uri(directory)) : [];

                // If the filter has names or extensions, use a function to filter the DirectoryMap
                if (has_files || has_directories || has_extensions)
                    reference.#options.filters[FILTER_TYPE] = (path, stats, strict) => {
                        // Perform strict matching based on the matchable candidates provided by the filter
                        const is_directory = stats.isDirectory();
                        if (is_directory) {
                            // We can only perform strict matching on a directory if we have some directory candidates to match against
                            if (has_directories) {
                                // Match one of the directory candidate uris as the ending of the provided path
                                // This essentially checks if the provided path is of one of the directory candidates
                                if (directory_uris.find((uri) => path.endsWith(uri))) return true;

                                // If we are in non-strict mode, we can perform loose matching with the directory candidates as the parents of the provided path
                                // This essentially checks if the current path directory is a child in the hierarchy of one of the directory candidates
                                if (!strict) {
                                    // Match each directory URI individually as a parent of the provided path
                                    for (const uri of directory_uris) {
                                        // URIs don't have trailing slashes so we need to add one to the end of the uri to make this a parent path check
                                        const [_, right] = path.split(`${uri}/`);

                                        // If we have some content on the right side of the split, then this is a child of the directory candidate
                                        if (right) return true;
                                    }
                                }
                            }
                        } else {
                            // Match one of the file candidate uris as the ending of the provided path
                            // This essentially checks if the provided path is of one of the file candidates
                            if (has_files && file_uris.find((uri) => path.endsWith(uri))) return true;

                            // Match one of the extension candidate uris as the ending of the provided path
                            // This essentially checks if the file at provided path is of one of the extension candidates
                            if (has_extensions) {
                                const name = path.split('/').slice(-1)[0];
                                if (extensions.find((extension) => match_extension(name, extension))) return true;
                            }
                        }

                        // Match one of the directory candidates as parent paths of the provided path
                        // This essentially prevents sub-directories and sub-files of a directory candidate from not being matched
                        if (has_directories) {
                            // Match each directory URI individually as a parent of the provided path
                            for (const uri of directory_uris) {
                                // URIs don't have trailing slashes so we need to add one to the end of the uri to make this a parent path check
                                const [_, right] = path.split(`${uri}/`);

                                // If we have some content on the right side of the split, then this is a child of the directory candidate
                                if (right) return true;
                            }
                        }

                        // At this point, none of the targeting filters from before matched this resource
                        // Let's attempt to do loose matching if strict matching is not required
                        if (strict) {
                            // Since strict matching is required, we can return false immediately without performing any loose matching
                            return false;
                        } else {
                            // Perform loose matching based on the matchable candidates provided by the filter
                            if (is_directory) {
                                // If this is a directory and the filter has not matchable directory candidates than this filter loosely matches everything
                                // Hence we can return a true match if the filter has no directory candidates to match against
                                return !has_directories;
                            } else {
                                // If this is a file and the filter has no matchable file or extension candidates than this filter loosely matches everything
                                // Hence we can return a true match if the filter has no file or extension candidates to match against
                                return !has_files && !has_extensions;
                            }
                        }
                    };
            }
        });

        // Initialize the chokidar watcher instance
        this._initialize_watcher().catch((error) => this.emit('error', error));

        // Initialize the cleanup interval to cleanup supressions
        this.#cleanup = setInterval(() => this._cleanup_supressions(), 1000 * 60);
    }

    /**
     * Destroys this DirectoryMap instance.
     *
     * @returns {Promise}
     */
    destroy() {
        this.#destroyed = true;
        clearInterval(this.#cleanup);
        return this.#watcher.close();
    }

    #ready_resolve;
    #ready_promise = new Promise((resolve) => (this.#ready_resolve = resolve));

    /**
     * Returns a Promise which is resolved once this DirecotryTree instance is ready to be used.
     * @returns {Promise<void>}
     */
    ready() {
        return this.#ready_promise;
    }

    /**
     * Returns the associated map record for the specified uri if it exists.
     *
     * @param {String} uri
     * @returns {MapRecord=}
     */
    get(uri) {
        return this.#directories[uri] || this.#files[uri];
    }

    /**
     * Cleans up expired supressions from the supressions map.
     * @private
     * @param {number} max_age_ms
     */
    _cleanup_supressions(max_age_ms = 1000) {
        // Cleanup any supressions that are older than the max age
        const now = Date.now();
        for (const key in this.#supressions) {
            const { updated_at } = this.#supressions[key];
            if (now - updated_at > max_age_ms) delete this.#supressions[key];
        }
    }

    /**
     * Supresses a future event from being emitted on a uri.
     *
     * @param {String} uri
     * @param {String} event
     * @param {Number} amount
     * @returns {Number} The amount of supressions for this uri/event.
     */
    supress(uri, event, amount = 1) {
        // Ignore temporary uris
        if (uri.startsWith('temporary://')) return;

        // Initialize the supression key or increment the amount of supressions
        const key = `${event}:${uri}`;
        if (!this.#supressions[key]) {
            this.#supressions[key] = {
                amount,
                updated_at: Date.now(),
            };
        } else {
            this.#supressions[key].amount += amount;
            this.#supressions[key].updated_at = Date.now();
        }
        return this.#supressions[key];
    }

    /**
     * Depresses a supressed event from being emitted on a uri.
     *
     * @private
     * @param {String} uri
     * @param {String} event
     * @param {Number} amount
     * @returns {Boolean} Whether the event was successfully de-pressed
     */
    _depress(uri, event, amount = 1) {
        // Decrement the amount of supressions and delete if it is less than 1
        const key = `${event}:${uri}`;
        if (this.#supressions[key]) {
            this.#supressions[key].amount -= amount;
            if (this.#supressions[key].amount < 1) delete this.#supressions[key];
            return true;
        }
        return false;
    }

    /**
     * Initializes the underlying watcher instance that will power this directory tree.
     * @private
     */
    async _initialize_watcher() {
        // Retrieve the root path from user options
        const path = this.#path;
        const { watcher } = this.#options;

        // Ensure the provided root path is accessible
        if (!(await is_accessible_path(path)))
            throw new Error(`new DirectoryMap(options.path) -> Unable to access the provided path: ${path}`);

        // Inject the top level filter callback into the watcher options
        const reference = this;
        const { _keep, _ignore } = this.#options.filters;
        watcher.ignored = (path, stats) => {
            // If this execution does not have stats avaialble then we cannot filter it hence we allow it
            if (stats === undefined) return false;

            // Always ignore the root path from being filtered
            if (path === reference.#path) return false;

            // Extrapolate the relative path for filtering this file/directory
            const relative = reference._relative_uri(path);

            // Assert the "ignore" filter as strict if one is available
            // The "ignore" filter is applied first as it is more restrictive and takes precedence over the "keep" filter
            if (typeof _ignore == 'function' && _ignore(relative, stats, true)) return true;

            // Assert the "keep" filter and return true to filter this file/directory if it fails
            if (typeof _keep == 'function' && !_keep(relative, stats, false)) return true;

            // If this candidate passes above filters, then it is good to be tracked
            return false;
        };

        // Initialize the chokidar watcher instance for this root path
        this.#watcher = Chokidar.watch(path, watcher);

        // Bind appropriate handlers to consume the watcher events
        this.#watcher.on('addDir', (path, stats) => this._on_directory_create(path, stats));
        this.#watcher.on('unlinkDir', (path) => this._on_directory_delete(path));
        this.#watcher.on('unlink', (path) => this._on_file_delete(path));

        // The ready event should wait for all asynchronous operations to complete
        const promises = [];
        this.#watcher.on('add', (path, stats) => {
            const promise = this._on_file_add(path, stats, stats.size > 0);
            if (reference.#ready_resolve) promises.push(promise);
        });

        this.#watcher.on('change', (path, stats) => {
            const promise = this._on_file_change(path, stats);
            if (reference.#ready_resolve) promises.push(promise);
        });

        // Wait for all pending promises to resolve before resolving the ready promise
        this.#watcher.on('ready', async () => {
            if (promises.length > 0) await Promise.all(promises);
            reference.#ready_resolve();
            reference.#ready_resolve = null;
        });

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
    _relative_uri(path) {
        // Retrieve the relative path by removing the root path from the provided path
        return to_forward_slashes(path).replace(this.#path, '');
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
            md5: stats.md5 || '',
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
        const relative_uri = this._relative_uri(path);

        // Ignore the root directory from being stored in directories map
        if (relative_uri.length == 0) return;

        // Return the formated object stats
        return {
            uri: relative_uri,
            path: to_forward_slashes(path),
            stats: this._filtered_stats(stats),
        };
    }

    /**
     * Asserts whether the provided path and stats are ignored by the watcher.
     *
     * @param {String} path
     * @param {FileSystem.Stats=} stats
     * @returns {boolean}
     */
    _is_watcher_ignored(path, stats) {
        return this.#options.watcher.ignored(path, stats);
    }

    /**
     * Handles the watcher directory create event.
     *
     * @private
     * @param {String} path
     * @param {FileSystem.Stats} stats
     */
    _on_directory_create(path, stats) {
        // Assert the watcher ignored on this directory
        if (this._is_watcher_ignored(path, stats)) return;

        // Retrieve the relative path to the directory
        const object = this._object_stats(path, stats);
        if (object) {
            // Expire schema & store the object by the relative path aka uri
            this.#schema = null;
            this.#directories[object.uri] = object;

            // Emit the directory create event if it is not supressed
            if (!this._depress(object.uri, 'directory_create', 1)) this.emit('directory_create', object.uri, object);
        }
    }

    /**
     * Handles the watcher directory delete event.
     *
     * @private
     * @param {String} path
     */
    _on_directory_delete(path) {
        // Assert the watcher ignored on this directory
        if (this._is_watcher_ignored(path)) return;

        // Retrieve the relative path to the directory
        const relative_uri = this._relative_uri(path);

        // Expire schema & delete the directory's record from the directory map
        this.#schema = null;
        delete this.#directories[relative_uri];

        // Emit the directory delete event if it is not supressed
        if (!this._depress(relative_uri, 'directory_delete', 1)) this.emit('directory_delete', relative_uri);
    }

    /**
     * Handles the watcher file add event.
     *
     * @private
     * @param {String} path
     * @param {FileSystem.Stats} stats
     * @param {Boolean} is_change
     */
    async _on_file_add(path, stats, is_change = false) {
        // Assert the watcher ignored on this file
        if (this._is_watcher_ignored(path, stats)) return;

        // Generate the MD5 hash for this file
        stats.md5 = stats.size > 0 ? await generate_md5_hash(path) : '';

        // Retrieve the relative path to the directory
        const object = this._object_stats(path, stats);
        if (object) {
            // Ensure the file size is less than the maximum file size limit
            const { max_file_size } = this.#options.limits;
            if (max_file_size && stats.size > max_file_size) {
                // Ensure we cleanup any existing record if the file size is too large
                if (this.#files[object.uri]) delete this.#files[object.uri];
                return this.emit('file_size_limit', object.uri, object);
            }

            // Expire schema & store the object by the relative path aka uri
            this.#schema = null;
            this.#files[object.uri] = object;

            // Emit the file add or change event if it is not supressed
            const event = is_change ? 'file_change' : 'file_create';
            if (!this._depress(object.uri, event, 1)) this.emit(event, object.uri, object);
        }

        // Emit the 'file_md5' event for any consumers that are listening for a md5 change
        // Do not supress this event as it is used for file integrity checks by consumers
        this.emit(`md5_change:${object.uri}`, object);
    }

    /**
     * Handles the watcher file delete event.
     *
     * @private
     * @param {String} path
     */
    _on_file_delete(path) {
        // Assert the watcher ignored on this file
        if (this._is_watcher_ignored(path)) return;

        // Retrieve the relative path to the directory
        const relative_uri = this._relative_uri(path);

        // Expire schema & delete the file's record from the directory map
        this.#schema = null;
        delete this.#files[relative_uri];

        // Emit the file delete event if it is not supressed
        if (!this._depress(relative_uri, 'file_delete', 1)) this.emit('file_delete', relative_uri);
    }

    /**
     * Handles the watcher file change event.
     *
     * @private
     * @param {String} path
     * @param {FileSystem.Stats} stats
     */
    async _on_file_change(path, stats) {
        // Pass through this event to the file add event but as a change event
        this._on_file_add(path, stats, true);
    }

    /* DirectoryMap Getters */

    /**
     * Returns whether this DirectoryMap instance is destroyed.
     * @returns {Boolean}
     */
    get destroyed() {
        return this.#destroyed;
    }

    /**
     * Returns the root path of this DirectoryMap.
     * @returns {String}
     */
    get path() {
        return this.#path;
    }

    #schema;

    /**
     * Returns a stringified JSON representation of this DirectoryMap as a schematic.
     * All candidates are sorted in increasing url parts.
     * Directories come first, files come second.
     * Directory Structure -> [uri: string]: [created_at: number, updated_at: number]
     * File Structure -> [uri: string]: [size: number, created_at: number, updated_at: number]
     * Note! Directories do NOT have a size property at index 0.
     *
     * @returns {Object<string, Array<string>>}
     */
    get schema() {
        // Resolve from local cache if available
        if (this.#schema) return this.#schema;

        // Build a new schema based on latest available directory/file candidates
        const schema = {};
        [this.#directories, this.#files].forEach((records, index) => {
            Object.keys(records)
                .sort((left, right) => {
                    // Sort the records in increasing url parts for hierarchy purposes
                    const leftParts = left.split('/');
                    const rightParts = right.split('/');
                    return leftParts.length - rightParts.length;
                })
                .forEach((uri) => {
                    // Convert each record into a simplified array
                    const record = records[uri];
                    const { md5, created_at, modified_at } = record.stats;

                    // Only include the size property for FILES only
                    schema[uri] = index == 0 ? [created_at, modified_at] : [md5, created_at, modified_at];
                });
        });

        // Cache and resolve the newly built schema
        this.#schema = schema;
        return this.#schema;
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

    /**
     * Returns all the supressed events in this DirectoryMap.
     * @returns {Object}
     */
    get supressions() {
        return this.#supressions;
    }
}
