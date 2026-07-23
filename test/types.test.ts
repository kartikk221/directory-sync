import {
    Mirror,
    Server,
    type Entry,
    type HostedDirectory,
    type MirrorOptions,
    type ServerOptions,
} from '../index.js';

const server_options: ServerOptions = {
    port: 8080,
    auth: 'secret',
    limits: { max_body_length: 1024 },
};
const server = new Server(server_options);
const hosted: Promise<HostedDirectory> = server.host('repository', '/tmp/repository', {
    filters: { keep: { extensions: ['.js'] } },
    state: { max_tombstones: 10_000 },
    watcher: { atomic: 250 },
});
void hosted.then(({ manager, map }) => {
    const entry: Entry | undefined = map.canonical_entry('/index.js');
    const content: Promise<Buffer> = manager.read('/index.js');
    void entry;
    void content;
});
void server.ready();
void server.unhost('repository');
void server.destroy();
server.on('error', (error: Error) => error.message);

const mirror_options: MirrorOptions = {
    path: '/tmp/mirror',
    hostname: '127.0.0.1',
    port: 8080,
    auth: 'secret',
    queue: {
        max_concurrent: 4,
        max_queued: 100,
        max_pending: 1_000,
        throttle: { rate: 20, interval: 1000 },
    },
};
const mirror = new Mirror('repository', mirror_options);
const mirror_entry: Entry | undefined = mirror.map?.canonical_entry('/index.js');
void mirror_entry;
void mirror.ready();
void mirror.destroy();
