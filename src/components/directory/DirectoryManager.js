import tempDirectoryRaw from 'temp-dir';
import Stream from 'stream';
import FileSystemSync from 'fs';
import FileSystem from 'fs/promises';

import { randomUUID } from 'crypto';
import { to_forward_slashes, generate_md5_hash } from '../../utils/operators.js';

const SYSTEM_TEMPORARY_PATH = to_forward_slashes(tempDirectoryRaw);

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
        // Return the temporary directory system path if the uri is a temporary uri
        if (uri.startsWith('temporary://')) {
            return `${SYSTEM_TEMPORARY_PATH}${uri.replace('temporary://', '/')}`;
        } else {
            return `${this.#map.path}${uri}`;
        }
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
     * @param {String} md5
     * @param {Boolean=} supress
     */
    async write(uri, data, md5, supress) {
        const path = this._absolute_path(uri);

        // Ensure this write operation has an accompanying md5 hash
        const has_length = data instanceof Stream.Readable || data.length > 0;
        if (typeof md5 !== 'string' || (has_length && md5.length == 0))
            throw new Error('MD5 Hash Is Required For Checksum Validation When Writing Content');

        // Suppress any upcoming file change events for this file
        if (supress || this.#supress_mutations) {
            this.#map.supress(to_uri, 'file_change', 1);
            this.#map.supress(to_uri, 'file_create', 1);
        }

        // Begin the filesystem operation with a consumable Promise
        let promise;
        if (data instanceof Stream.Readable) {
            // Create a writable stream and pipe the provided stream into it
            const writable = FileSystemSync.createWriteStream(path);
            data.pipe(writable);
            promise = new Promise((resolve) => writable.on('finish', resolve));
        } else {
            promise = FileSystem.writeFile(path, data);
        }

        // Determine if we need to perform an integrity check with the provided md5
        if (md5) {
            // Wait for the filesystem operation to finish
            await promise;

            // If this is temporary file, generate the md5 hash from the file path
            // else wait for the Directory Map to load the new md5 hash
            const written_md5 = uri.startsWith('temporary://')
                ? await generate_md5_hash(path)
                : await new Promise((resolve) =>
                      this.#map.once(`md5_change:${uri}`, ({ stats }) => resolve(stats.md5))
                  );

            // Throw an error if the md5's do not match
            if (md5 !== written_md5) throw new Error(`ERR_FILE_WRITE_INTEGRITY_CHECK_FAILED`);
        } else {
            return await promise;
        }
    }

    /**
     * Performs an indirect write by first creating a temporary file and then moving it to the final destination.
     * Any errors occured during the writing of the temporary file are thrown safely.
     *
     * @param {String} uri
     * @param {String|Buffer|Stream.Readable} data
     * @param {String=} md5
     * @param {Boolean=} supress
     */
    async indirect_write(uri, data, md5, supress) {
        // If this is an empty file, we can just write it directly
        if (!(data instanceof Stream.Readable) && data.length === 0) return await this.write(uri, data, md5);

        // Attempt to write the file safely to a temporary file
        const temp_uri = `temporary://temp-${randomUUID()}`;
        try {
            await this.write(temp_uri, data, md5);
        } catch (error) {
            // Delete the temporary file as we encountered an error
            await this.delete(temp_uri, true);
            throw error;
        }

        // Move the temporary file to the final destination
        await this.move(temp_uri, uri, supress);
    }

    /**
     * Moves the file or directory at the specified uri to the specified destination.
     *
     * @param {String} from_uri
     * @param {String} to_uri
     * @param {Boolean=} supress
     */
    async move(from_uri, to_uri, supress) {
        // Determine the absolute paths for the source and destination
        const from_path = this._absolute_path(from_uri);
        const to_path = this._absolute_path(to_uri);

        // Suppress any upcoming file change events for this file
        if (supress || this.#supress_mutations) {
            this.#map.supress(to_uri, 'file_change', 1);
            this.#map.supress(to_uri, 'file_create', 1);
        }

        // Copy the file from the source to the destination
        await FileSystem.copyFile(from_path, to_path);

        // Delete the source file
        await FileSystem.rm(from_path, {
            force: true,
        });
    }

    /**
     * Deletes a file or directory at the specified uri.
     *
     * @param {String} uri
     * @param {Boolean} is_directory
     * @param {Boolean=} supress
     * @returns {Promise=}
     */
    delete(uri, is_directory = false, supress) {
        if (this.#map.get(uri)) {
            const path = this._absolute_path(uri);
            if (supress || this.#supress_mutations)
                this.#map.supress(uri, is_directory ? 'directory_delete' : 'file_delete', 1);
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
