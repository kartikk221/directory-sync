import Crypto from 'crypto';
import FileSystem from 'fs';
import AsyncFileSystem from 'fs/promises';

/**
 * Writes values from focus object onto base object.
 *
 * @param {Object} obj1 Base Object
 * @param {Object} obj2 Focus Object
 */
function wrap_object(original, target) {
    Object.keys(target).forEach((key) => {
        const old_value = original[key];
        const new_value = target[key];
        if (new_value !== null && typeof new_value == 'object' && Array.isArray(new_value)) {
            if (old_value === null || typeof old_value !== 'object') original[key] = {};
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
    if (final == undefined) return new Promise((resolve, rej) => async_for_each(items, handler, cursor, resolve));
    if (cursor < items.length) return handler(items[cursor], () => async_for_each(items, handler, cursor + 1, final));
    return final(); // Resolve master promise
}

/**
 * Matches the provided file name against the provided extension.
 *
 * @private
 * @param {String} name
 * @param {String} extension
 * @returns {Boolean}
 */
function match_extension(name, extension) {
    return name.endsWith(extension.startsWith('.') ? extension : `.${extension}`);
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

/**
 * Returns whether the provided path is accessible and valid.
 *
 * @param {String} path
 * @returns {Promise<boolean>}
 */
async function is_accessible_path(path) {
    try {
        const { F_OK, W_OK } = FileSystem.constants;
        await AsyncFileSystem.access(path, F_OK | W_OK);
    } catch (error) {
        return false;
    }
    return true;
}

/**
 * Generates and returns a MD5 hash of the file at provided path.
 *
 * @param {String} path
 * @returns {Promise<string>}
 */
function generate_md5_hash(path) {
    return new Promise((resolve, reject) => {
        try {
            // Initialize a md5 hash and a readable stream for file at specified path
            const hash = Crypto.createHash('md5');
            const stream = FileSystem.createReadStream(path);

            // Safely pipe the incoming stream to the hash
            stream.once('error', reject);
            stream.on('data', (data) => hash.update(data));
            stream.on('close', () => resolve(hash.digest('hex')));
        } catch (error) {
            reject(error);
        }
    });
}

/**
 * Safely parses a string into a JSON object.
 *
 * @param {String} string
 * @returns {Object=}
 */
function safe_json_parse(string) {
    let json;
    try {
        json = JSON.parse(string);
    } catch (error) {}
    if (json) return json;
}

/**
 * Converts ASCII string to Hexadecimal string.
 *
 * @param {String} string
 * @returns {String}
 */
function ascii_to_hex(string) {
    return Buffer.from(string).toString('hex');
}

/**
 * Converts Hexadecimal string to ASCII string.
 *
 * @param {String} string
 * @returns {String}
 */
function hex_to_ascii(string) {
    return Buffer.from(string, 'hex').toString('ascii');
}

export {
    wrap_object,
    async_wait,
    async_for_each,
    match_extension,
    to_forward_slashes,
    is_accessible_path,
    generate_md5_hash,
    safe_json_parse,
    ascii_to_hex,
    hex_to_ascii,
};
