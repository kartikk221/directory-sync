const DirectorySync = require('../index.js');
const Source = new DirectorySync.Source({
    port: 8080,
});

Source.host('TEST_DIRECTORY', './source');
Source.on('log', console.log);

setTimeout(() => {
    const Editor = new DirectorySync.Editor({
        identity: 'TEST_DIRECTORY',
        path: './cloned',
        host: 'localhost',
        port: 8080,
    });

    // Editor.on('log', console.log);
}, 500);
