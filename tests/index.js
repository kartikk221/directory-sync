const DirectorySync = require('../index.js');

const TestServer = new DirectorySync.Server({
    port: 8080,
});

(async () => {
    await TestServer.host('source', './source');
    console.log(TestServer.hosts['source'].map.directories);
    console.log(TestServer.hosts['source'].map.files);
    console.log(TestServer.hosts['source'].map.schema);
})();
