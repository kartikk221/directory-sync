import Stream from 'stream';
import FileSystemSync from 'fs';
import FileSystem from 'fs/promises';

export default class DirectoryManager {
    #map;
    #supress_mutations = true;
    constructor(map, supress_mutations = true) {
        this.#map = map;
        this.#supress_mutations = supress_mutations;
    }

    /**
     * Returns the absolute system path for the provided relative uri.
     *
     * @param {String} uri
     * @returns {String}
     */
    _absolute_path(uri) {
        return `${this.#map.path}${uri}`;
    }

    /**
     * Creates a file or directory at the specified uri.
     *
     * @param {String} uri
     * @param {Boolean} is_directory
     * @returns {Promise=}
     */
    create(uri, is_directory = false) {
        if (!this.#map.get(uri)) {
            const path = this._absolute_path(uri);
            if (is_directory) {
                if (this.#supress_mutations) this.#map.supress(uri, 'directory_create', 1);
                return FileSystem.mkdir(path);
            } else {
                if (this.#supress_mutations) this.#map.supress(uri, 'file_create', 1);
                return FileSystem.writeFile(path, '');
            }
        }
    }

    /**
     * Provides direct content or a readable stream for a file at the specified uri.
     *
     * @param {String} uri
     * @returns {Stream.Readable|Promise}
     */
    read(uri, stream = false) {
        const path = this._absolute_path(uri);
        if (stream) {
            return FileSystemSync.createReadStream(path);
        } else {
            return FileSystem.readFile(path);
        }
    }

    /**
     * Writes/Streams content to a file at the specified uri.
     *
     * @param {String} uri
     * @param {String|Buffer|Stream.Readable} data
     * @returns {Promise}
     */
    write(uri, data) {
        const path = this._absolute_path(uri);

        // Suppress the file_change event if file already exists locally
        if (this.#map.get(uri)) {
            if (this.#supress_mutations) this.#map.supress(uri, 'file_change', 1);
        } else {
            if (this.#supress_mutations) this.#map.supress(uri, 'file_create', 1);
        }

        if (data instanceof Stream.Readable) {
            // Create a writable stream and pipe the provided stream into it
            const writable = FileSystemSync.createWriteStream(path);
            data.pipe(writable);
            return new Promise((resolve) => writable.on('finish', resolve));
        } else {
            return FileSystem.writeFile(path, data);
        }
    }

    /**
     * Deletes a file or directory at the specified uri.
     *
     * @param {String} uri
     * @param {Boolean} is_directory
     * @returns {Promise=}
     */
    delete(uri, is_directory = false) {
        if (this.#map.get(uri)) {
            const path = this._absolute_path(uri);
            if (this.#supress_mutations) this.#map.supress(uri, is_directory ? 'directory_delete' : 'file_delete', 1);
            return new Promise((resolve, reject) =>
                FileSystem.rm(path, {
                    recursive: true,
                })
                    .then(resolve)
                    .catch((error) => {
                        // Supress 'ENOENT' errors
                        if (error.code === 'ENOENT') resolve();
                        reject(error);
                    })
            );
        }
    }
}
