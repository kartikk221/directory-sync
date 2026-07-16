# DirectorySync: Multi-Directional FileSystem Synchronization
Synchronize directories between multiple machines over network with the ability to be synchronized in real-time.

## Installation
DirectorySync requires an even-numbered Node.js release supported by HyperExpress v7: Node.js 22, 24, or 26.
```
npm i directory-sync
```

HyperExpress uses the native uWebSockets.js addon. Use a supported macOS or Windows platform, or a glibc-based Linux distribution such as Debian or Ubuntu. Alpine Linux is not supported by HyperExpress v7, including Alpine installations using `gcompat`.

DirectorySync v3 uses Chokidar 5.0.0. The LTS write-stability defaults are retained: `awaitWriteFinish` waits for a file's size to remain stable for 500 ms, and atomic-write coalescing is enabled for the default `fs.watch` backend. Explicit watcher options continue to take precedence. Polling remains available for network and non-standard filesystems.

## Synchronization Model

Each `Server` host is the central authority for one repository. Any number of `Mirror` nodes may push local changes to that authority and pull accepted changes from it. The authority resolves conflicting active entries using their filesystem `mtime`: the entry with the newer modification time wins, while the authority keeps its current entry on an exact timestamp tie.

Deletions are represented by durable tombstones instead of by absence. A directory tombstone also covers its older descendants, while newer descendants are preserved. Each node retains up to 10,000 tombstones by default and evicts the oldest record when the limit is exceeded. Active entry metadata and tombstones are stored under `.directory-sync` inside the synchronized root so deletions made while a process is stopped can be recovered on restart.

The authority protocol changed to v3 to carry timestamps, SHA-256 hashes, actor identities, and tombstones. All servers and mirrors for a repository must therefore be upgraded together from DirectorySync v2. Existing `Server` and `Mirror` constructor options, methods, getters, filters, events, and queue controls remain supported unless the new authority semantics require different conflict results.

## Upgrading From The V2 LTS Release

Most applications can keep their existing `Server` and `Mirror` construction code. Upgrade every server and mirror for a repository together, use a supported Node.js release, and restart the fleet. DirectorySync will create its state directory automatically and perform the initial reconciliation.

The intentionally breaking requirements are:

- Node.js 14-21 are no longer supported. HyperExpress v7 requires a supported modern runtime; DirectorySync supports Node.js 22, 24, and 26.
- HyperExpress 7 uses a newer native uWebSockets.js build. Alpine Linux is no longer supported; use macOS, Windows, or glibc-based Linux.
- Protocol v2 and v3 peers cannot communicate. V3 peers send timestamps, SHA-256 hashes, actor IDs, canonical entries, and tombstones, so a repository must be upgraded as one fleet.
- Symbolic links are no longer followed or synchronized. This prevents a repository path from escaping its configured root.
- `.directory-sync` is reserved for durable local state unless `state.path` points elsewhere. A repository entry with that top-level name is rejected.

Behavior differences in v3 are:

- A `Server` host is the central authority. Mirrors may push newer local entries and pull accepted authority entries; an exact mtime tie always keeps the authority's current entry.
- Deletion is durable state rather than simple absence. Up to 10,000 tombstones are retained by default, directory tombstones cover older descendants, and the oldest retained tombstone is evicted first.
- File integrity uses streamed SHA-256 and byte-length validation instead of MD5. The backward-compatible `schema` and `md5_change:*` surfaces remain, but their hash value is SHA-256.
- File writes are verified into a same-filesystem temporary file and atomically renamed. Partial, oversized, stale, or checksum-invalid transfers never replace canonical content.
- File/directory type replacement, zero-byte files, offline deletions, and newer resurrection are explicitly supported and converge through the same conflict rules.
- Chokidar 5 replaces Chokidar 3. It is ESM-only, removes glob expansion from watched paths, and substantially reduces transitive dependencies. DirectorySync watches the configured root rather than a user glob, so normal `Server` and `Mirror` usage is unchanged.
- DirectorySync no longer installs global process-exit or uncaught-exception handlers. Applications retain control over their own lifecycle and should call `destroy()` during graceful shutdown.

Additive API improvements include `ready()` and idempotent `destroy()` lifecycle methods, `Server.unhost()`, protocol-v3 manifest access through `Mirror.map`, and bundled TypeScript declarations. Existing names—including the legacy `supress` spelling and convenience methods—remain available.

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
