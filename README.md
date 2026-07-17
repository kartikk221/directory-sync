# DirectorySync: Multi-Directional FileSystem Synchronization
Synchronize directories between multiple machines over network with the ability to be synchronized in real-time.

## Installation
DirectorySync requires an even-numbered Node.js release supported by HyperExpress v7: Node.js 22, 24, or 26.
```
npm i directory-sync
```

HyperExpress uses the native uWebSockets.js addon. Use a supported macOS or Windows platform, or a glibc-based Linux distribution such as Debian or Ubuntu. Alpine Linux is not supported by HyperExpress v7, including Alpine installations using `gcompat`.

DirectorySync uses Chokidar 5.0.0. The LTS write-stability defaults are retained: `awaitWriteFinish` waits for a file's size to remain stable for 500 ms, and atomic-write coalescing is enabled for the default `fs.watch` backend. Explicit watcher options continue to take precedence. Polling remains available for network and non-standard filesystems.

## Synchronization Model

Each `Server` host is the central authority for one repository. Any number of `Mirror` nodes may push local changes to that authority and pull accepted changes from it. During one Server lifetime, every accepted mutation receives a Server-issued, increment-only revision. A Mirror mutation based on the current revision is accepted without clock arbitration; stale or unknown revisions use filesystem `mtime` once to resolve contention, with the authority keeping its current entry on an exact timestamp tie. The accepted result then receives a new revision and cannot be reopened by timestamp differences.

The active manifest is rebuilt directly from each filesystem on startup. DirectorySync creates no private state directory, manifest cache, tombstone file, or persistent temporary directory. Verified downloads use the OS temporary directory and fall back to a UUID-named sibling when that directory is unavailable or a cross-filesystem move requires one.

Deletions are represented by Server-only, in-memory tombstones. A directory tombstone covers older descendants, newer descendants are preserved, and redundant older descendant tombstones are removed. Each Server host retains up to 10,000 tombstones by default and evicts the oldest timestamp when the limit is exceeded. Server restart intentionally creates a new authority epoch and clears revisions and deletion history.

Protocol v5 carries a process-lifetime Server epoch, authority revisions, filesystem timestamps, SHA-256 hashes, actor identities, clock information, and tombstones. Mirrors keep observed revisions only in memory and never mint authority revisions. They normalize filesystem times into the Server's clock domain only for cold-start or stale-revision fallback, then translate accepted times back before applying them with `utimes()`. All servers and mirrors for a repository must be upgraded together.

### Reconciliation

On initial connection and reconnect, a Mirror scans its filesystem while establishing its Server subscription. After subscription is confirmed, it fetches one response containing the active manifest, tombstones, protocol version, and Server time. Reconciliation applies deletions first, creates directories from shallowest to deepest, transfers up to 100 files concurrently by default, and finally restores directory mtimes from deepest to shallowest. The existing `queue.max_concurrent` option can lower or raise that limit for a particular host. Transfers remain streaming and backpressure-aware, so concurrency does not buffer entire files in memory. WebSocket events provide low-latency updates, while complete reconciliation remains the recovery mechanism.

For each path, a matching base revision establishes causal order without consulting timestamps. If revision history is unavailable or stale, the newest normalized timestamp wins once: active files and directories use `mtime`, tombstones use their deletion time, and exact ties keep the Server entry. Matching file size and SHA-256 avoid content transfer and simply adopt the Server revision. Accepted timestamps are applied to the filesystem, so copying a file does not replace its logical modification time with its transfer time.

### Performance

File transfers are streamed with backpressure and default to 100 concurrent files through `queue.max_concurrent`. Manifest hashing is separately limited to at most four files by default through `hashing.max_concurrent`, avoiding unbounded random disk reads. Hashes are reused only in memory while a process is running and only while both file size and `mtime` remain unchanged; no hash or manifest cache is persisted.

`npm run benchmark` runs the bidirectional stress scenario: 100 distinct random 1 MiB files begin across both the Server and Mirror, converge with full streamed integrity verification, then receive parallel overwrites and deletions from both sides followed by another complete integrity check. `BENCHMARK_FILES`, `BENCHMARK_FILE_SIZE`, and `BENCHMARK_CONCURRENCY` can adjust the workload.

### Tombstones And Deletion Limits

- A live Server or Mirror deletion creates a typed tombstone on the Server. File tombstones cover the exact path, while directory tombstones also cover older entries in the deleted subtree.
- A newer entry may resurrect a deleted path. The previous typed tombstone can remain alongside that active entry so stale peers cannot restore the older deleted type or subtree.
- Directory tombstones discard only descendant tombstones that are not newer than the directory deletion. Later descendant deletions remain independently effective.
- A deletion made while a Mirror process is stopped cannot be distinguished from a file that never existed. On reconnect, an existing Server entry is therefore restored to that Mirror.
- Restarting the Server or evicting an old tombstone removes that deletion knowledge. A sufficiently stale Mirror may then restore previously deleted content.

The default limit is 10,000 effective tombstones per hosted directory. It can be changed with `state.max_tombstones`.

Clock normalization handles a consistently offset Mirror clock, including large offsets, when timestamp fallback is required. Offsets within the timing uncertainty of the manifest request are treated as zero so network jitter cannot turn an exact timestamp tie into a newer Mirror mutation. Files copied in with foreign preserved timestamps or a clock that changes substantially between runs remain subject to normal filesystem-mtime limitations after an epoch change because DirectorySync deliberately stores no timestamp or revision provenance outside process memory.

## Upgrading From The V2 LTS Release

Most applications can keep their existing `Server` and `Mirror` construction code. Upgrade every server and mirror for a repository together, use a supported Node.js release, and restart the fleet. DirectorySync rebuilds each manifest from the filesystem and performs the initial reconciliation automatically.

The intentionally breaking requirements are:

- Node.js 14-21 are no longer supported. HyperExpress v7 requires a supported modern runtime; DirectorySync supports Node.js 22, 24, and 26.
- HyperExpress 7 uses a newer native uWebSockets.js build. Alpine Linux is no longer supported; use macOS, Windows, or glibc-based Linux.
- Protocol v5 peers cannot communicate with older protocol versions, so a repository must be upgraded as one fleet.
- Symbolic links are no longer followed or synchronized. This prevents a repository path from escaping its configured root.
- DirectorySync no longer reserves `.directory-sync` or writes any private state inside the synchronized root.

Behavior differences in v5 are:

- A `Server` host is the central authority. Server-issued revisions establish causal order while it remains alive; timestamp fallback is limited to stale or unknown revision history, and an exact fallback tie keeps the authority's current entry.
- Deletion history lives only in Server memory. Deletions made while a Mirror is stopped cannot be inferred, and restarting the Server clears all tombstones.
- File integrity uses streamed SHA-256 and byte-length validation instead of MD5. The backward-compatible `schema` and `md5_change:*` surfaces remain, but their hash value is SHA-256.
- File writes are verified in the OS temporary directory before replacement. Cross-filesystem moves stage one verified sibling for the final rename. Partial, oversized, stale, or checksum-invalid transfers never replace canonical content.
- File/directory type replacement, zero-byte files, live deletions, and newer resurrection converge through the same conflict rules.
- Chokidar 5 replaces Chokidar 3. It is ESM-only, removes glob expansion from watched paths, and substantially reduces transitive dependencies. DirectorySync watches the configured root rather than a user glob, so normal `Server` and `Mirror` usage is unchanged.
- DirectorySync no longer installs global process-exit or uncaught-exception handlers. Applications retain control over their own lifecycle and should call `destroy()` during graceful shutdown.

Additive API improvements include `ready()` and idempotent `destroy()` lifecycle methods, `Server.unhost()`, manifest access through `Mirror.map`, and bundled TypeScript declarations. Existing names—including the legacy `supress` spelling and convenience methods—remain available.

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
