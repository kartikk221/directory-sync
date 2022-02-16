const DirectorySync = require('../index.js');

const TestServer = new DirectorySync.Server({
    port: 8080,
    auth: {
        headers: {
            'x-key': 'development',
        },
    },
});

const TestMirror = new DirectorySync.Mirror('source', {
    path: './mirrored',
    port: 8080,
});

(async () => {
    await TestServer.host('source', './source');
})();
