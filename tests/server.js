import { Server } from '../index.js';

// Create a server instance to host source directory
const SERVER = new Server({
    port: 8080,
    auth: 'development',
});

// Bind appropriate listeners to Server
SERVER.on('log', (code, message) => console.log(`[SERVER][${code}] ${message}`));
SERVER.on('error', (error) => console.log(`[SERVER]`, error));

// Host the source directory
SERVER.host('source', './source', {
    filters: {
        ignore: {
            files: ['secret.json'],
            directories: ['assets/secret'],
        },
    },
});
