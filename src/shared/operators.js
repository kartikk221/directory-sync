const FileSystem = require('fs');

/**
 * Writes values from focus object onto base object.
 *
 * @param {Object} obj1 Base Object
 * @param {Object} obj2 Focus Object
 */
function wrap_object(original, target) {
    Object.keys(target).forEach((key) => {
        if (typeof target[key] == 'object') {
            if (original[key] === null || typeof original[key] !== 'object') original[key] = {};
            wrap_object(original[key], target[key]);
        } else {
            original[key] = target[key];
        }
    });
}

/**
 * Returns a promise which is resolved after specified delay number.
 *
 * @param {Number} delay
 * @returns {Promise} Promise
 */
function async_wait(delay = 0) {
    return new Promise((res, rej) => setTimeout(res, delay));
}

/**
 * This method can be used to create an asynchronous forEach loop which resolves on each iteration using the next() callback.
 *
 * @param {Array} items Example: ['some', 'word', 'word2']
 * @param {Function} handler Example: (item, next) => { (Your Code); next(); }
 * @returns {Promise} Resolves once looping is complete over all items.
 */
function async_for_each(items, handler, cursor = 0, final) {
    if (final == undefined)
        return new Promise((resolve, rej) => async_for_each(items, handler, cursor, resolve));
    if (cursor < items.length)
        return handler(items[cursor], () => async_for_each(items, handler, cursor + 1, final));
    return final(); // Resolve master promise
}

/**
 * Determines whether a path is accessible or not by FileSystem package.
 *
 * @param {String} path
 * @returns {Promise}
 */
function accessible_path(path) {
    return new Promise((resolve, reject) => {
        // Destructure constants for determine read & write codes
        const CONSTANTS = FileSystem.constants;
        const IS_VALID = CONSTANTS.F_OK;
        const HAS_PERMISSION = CONSTANTS.W_OK;
        FileSystem.access(path, IS_VALID | HAS_PERMISSION, (error) => {
            if (error) return resolve(false);
            resolve(true);
        });
    });
}

function create_directory(path) {
    return new Promise((resolve, reject) => {
        FileSystem.mkdir(path, (error) => {
            if (error) return reject(error);
            resolve();
        });
    });
}

function write_file(path, data) {
    return new Promise((resolve, reject) => {
        FileSystem.writeFile(path, data, (error) => {
            if (error) return reject(error);
            resolve();
        });
    });
}

function delete_path(path) {
    return new Promise((resolve, reject) => {
        FileSystem.rm(
            path,
            {
                recursive: true,
                retryDelay: 100,
                maxRetries: 20,
            },
            (error) => {
                if (error && error.toString().indexOf('no such file or directory') == -1)
                    return reject(error);
                resolve();
            }
        );
    });
}

/**
 * Converts any backslashes to forward slashes.
 *
 * @param {String} path
 * @returns {String}
 */
function to_forward_slashes(path) {
    return path.split('\\').join('/');
}

module.exports = {
    wrap_object,
    async_wait,
    accessible_path,
    create_directory,
    write_file,
    delete_path,
    async_for_each,
    to_forward_slashes,
};
