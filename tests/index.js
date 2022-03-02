import * as DirectorySync from '../index.js';

const TestServer = new DirectorySync.Server({
    port: 8080,
    auth: 'development',
});

// Bind appropriate listeners to Server
TestServer.on('log', (code, message) => console.log(`[SERVER] ${code} -> ${message}`));
TestServer.on('error', (error) => console.log(`[SERVER]`, error));

(async () => {
    // Initialize server host
    await TestServer.host('source', './source');

    // Initialize a mirror
    const TestMirror = new DirectorySync.Mirror('source', {
        path: './mirrored',
        hostname: 'localhost',
        port: 8080,
        auth: 'development',
    });

    // Bind appropriate listeners to Mirror
    TestMirror.on('log', (code, message) => console.log(`[MIRROR] ${code} -> ${message}`));
    TestMirror.on('error', (error) => console.log(`[MIRROR]`, error));
})();
