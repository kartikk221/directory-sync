const FileSystem = require('fs/promises');

class DirectoryManager {
    #map;
    constructor(map) {
        this.#map = map;
    }

    async read(uri) {}

    async write(uri, data) {}

    async delete(uri) {}
}

module.exports = DirectoryManager;
