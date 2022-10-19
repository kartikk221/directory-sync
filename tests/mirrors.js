import { Mirror } from '../index.js';

const NUM_MIRRORS_TO_CREATE = 3;

async function create_mirror(id) {
    const prefix = 'MIRROR-' + id;
    const mirror = new Mirror('source', {
        path: `./mirrored-${id}`,
        hostname: '127.0.0.1',
        port: 8080,
        auth: 'development',
    });

    // Bind appropriate listeners to Mirror
    mirror.on('log', (code, message) => console.log(`[${prefix}][${code}] ${message}`));
    mirror.on('error', (error) => console.log(`[${prefix}]`, error));
}

// Create a number of mirrors to test multi-directional sync
for (let i = 0; i < NUM_MIRRORS_TO_CREATE; i++) create_mirror(i + 1);
