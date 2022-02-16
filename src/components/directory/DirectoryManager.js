import FileSystem from 'fs/promises';

export default class DirectoryManager {
    #map;
    constructor(map) {
        this.#map = map;
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
     * Creates a file/directory at the specified uri.
     *
     * @param {String} uri
     * @param {Boolean} is_directory
     * @returns
     */
    create(uri, is_directory = false) {
        const path = this._absolute_path(uri);
        if (is_directory) {
            return FileSystem.mkdir(path);
        } else {
            return FileSystem.writeFile(path, '');
        }
    }

    async read(uri) {}

    async write(uri, data) {}

    async delete(uri) {}
}
