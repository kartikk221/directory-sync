# DirectorySync: Multi-Directional FileSystem Synchronization
Synchronize directories between multiple machines over network with the ability to be synchronized in real-time. (In Development)

## Installation
DirectorySync requires Node.js version 14+ and can be installed using Node Package Manager (npm).
```
npm i directory-sync
```

## Example: How To Setup A Server?
Below is a simple snippet for setting up a server and hosting directories which can then be mirrored on remote machines.

```javascript
import { Server } from 'directory-sync';

// Create a Server instance which will host your desired directories
const server = new Server({
   port: 8080,
   auth: 'some-secret-key' // You may use this key to authentice mirrors
});

// Bind approriate listeners for logging/error handling on the Server instance
server.on('log', (code, message) => console.log(`[SERVER][${code}] ${message}`));
server.on('error', (error) => console.log(`[SERVER]`, error));

// Host a directory with a custom "name" and then the "path" to the directory on your system
server.host('my-directory-1', './path/to/some-directory');

// You may also specify which files to keep/ignore with filters when hosting a directory
server.host('my-directory-2', './path/to/some-directory-2', {
    filters: {
        keep: {
            files: ['configuration.json'], // We also want to host this one configuration.json file
            extensions: ['.html', '.css', '.js'] // We only want to host HTML, CSS & JS files
        },
        ignore: {
            directories: ['node_modules'] // We do not want to host any files from node modules
        }
    }
});
```

## Example: How To Setup Mirrors?
Below is a simple snippet for setting up a mirror and synchronizing a remote directory from the example above.

```javascript
import { Mirror } from 'directory-sync';

// Create a mirror instance to mirror the "my-directory-2" directory from above example
const my_directory_2_mirror = new Mirror('my-directory-2', {
    path: './mirrored-some-directory-2', // Provide the path where this directory should be mirrored
    hostname: '127.0.0.1', // IP/Host address of the Server instance machine
    port: 8080, // Port of the Server instance machine
    auth: 'some-secret-key', // Authentication key to access the Server instance
});

// Bind approriate listeners for logging/error handling on the Mirror instance
my_directory_2_mirror.on('log', (code, message) => console.log(`[MIRROR][${code}] ${message}`));
my_directory_2_mirror.on('error', (error) => console.log(`[MIRROR]`, error));
```