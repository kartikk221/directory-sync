import * as DirectorySync from '../index.js';

const TestServer = new DirectorySync.Server({
    port: 8080,
    auth: 'development',
});

// Bind appropriate listeners to Server
TestServer.on('log', (code, message) => console.log(`[SERVER][${code}] ${message}`));
TestServer.on('error', (error) => console.log(`[SERVER]`, error));

(async () => {
    // Initialize server host
    await TestServer.host('source', './source');

    // Initialize multiple mirrors
    ['./mirrored', './mirrored-2', './mirrored-3'].forEach((path) => {
        const prefix = `MIRROR:${path}`;
        const TestMirror = new DirectorySync.Mirror('source', {
            path,
            hostname: 'localhost',
            port: 8080,
            auth: 'development',
        });

        // Bind appropriate listeners to Mirror
        TestMirror.on('log', (code, message) => console.log(`[${prefix}][${code}] ${message}`));
        TestMirror.on('error', (error) => console.log(`[${prefix}]`, error));
    });
})();
