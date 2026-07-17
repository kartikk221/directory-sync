import assert from 'node:assert/strict';
import Crypto from 'node:crypto';
import FileSystem from 'node:fs/promises';
import Path from 'node:path';
import { Readable } from 'node:stream';
import { setTimeout as wait } from 'node:timers/promises';
import test from 'node:test';
import DirectoryManager from '../src/components/directory/DirectoryManager.js';
import DirectoryMap from '../src/components/directory/DirectoryMap.js';
import { capture_errors, sha256, temporary_directory, wait_for, write_file } from './helpers.js';

const WATCHER = { awaitWriteFinish: false, interval: 10, usePolling: true };

async function create_map(t, root, options = {}) {
    const map = new DirectoryMap({ path: root, watcher: WATCHER, ...options });
    const errors = capture_errors(map);
    t.after(() => map.destroy());
    await map.ready();
    assert.deepEqual(errors, []);
    return { errors, manager: new DirectoryManager(map, true), map };
}

test('DirectoryMap indexes files, directories, filters, and user-owned hidden paths', async (t) => {
    const root = await temporary_directory(t);
    await write_file(root, '/keep/data.txt', 'kept');
    await write_file(root, '/keep/data.bin', 'ignored');
    await write_file(root, '/ignored/data.txt', 'ignored');
    await write_file(root, '/.directory-sync/user.txt', 'user-owned');
    const { map } = await create_map(t, root, {
        filters: {
            keep: { extensions: ['.txt'] },
            ignore: { directories: ['ignored'] },
        },
    });

    assert.equal(map.get('/keep/data.txt').stats.sha256, sha256('kept'));
    assert.equal(map.get('/keep/data.bin'), undefined);
    assert.equal(map.get('/ignored/data.txt'), undefined);
    assert.equal(map.get('/.directory-sync/user.txt').stats.sha256, sha256('user-owned'));
    assert.equal(map.schema['/keep/data.txt'][0], sha256('kept'));
});

test('DirectoryManager validates streamed size and checksum before atomic replacement', async (t) => {
    const root = await temporary_directory(t);
    await write_file(root, '/file.txt', 'original');
    const { manager, map } = await create_map(t, root);
    const modified_at = Date.now() + 1_000;
    const content = Buffer.from('replacement');
    await manager.apply_file('/file.txt', content, {
        type: 'file',
        modified_at,
        size: content.length,
        sha256: sha256(content),
    });
    assert.equal(await FileSystem.readFile(Path.join(root, 'file.txt'), 'utf8'), 'replacement');
    assert.equal(map.get_entry('/file.txt').modified_at, modified_at);

    await assert.rejects(
        manager.apply_file('/file.txt', 'corrupt', {
            type: 'file',
            modified_at: modified_at + 1,
            size: 7,
            sha256: '0'.repeat(64),
        }),
        { code: 'CHECKSUM' }
    );
    assert.equal(await FileSystem.readFile(Path.join(root, 'file.txt'), 'utf8'), 'replacement');

    await assert.rejects(
        manager.apply_file('/file.txt', 'short', {
            type: 'file',
            modified_at: modified_at + 2,
            size: 99,
            sha256: sha256('short'),
        }),
        { code: 'INVALID_SIZE' }
    );
});

test('DirectoryManager safely replaces file/directory type conflicts without watcher echoes', async (t) => {
    const root = await temporary_directory(t);
    await write_file(root, '/entry', 'file');
    const { errors, manager, map } = await create_map(t, root);
    await manager.apply_directory('/entry', { type: 'directory', modified_at: Date.now() + 1_000 });
    await wait(100);
    assert.equal(map.get_entry('/entry').type, 'directory');

    const content = Buffer.from('again');
    await manager.apply_file('/entry', content, {
        type: 'file',
        modified_at: Date.now() + 2_000,
        size: content.length,
        sha256: sha256(content),
    });
    await wait(100);
    assert.equal(map.get_entry('/entry').type, 'file');
    assert.equal(await FileSystem.readFile(Path.join(root, 'entry'), 'utf8'), 'again');
    assert.deepEqual(errors, []);
});

test('non-self directory tombstones preserve the directory and newer descendants', async (t) => {
    const root = await temporary_directory(t);
    const base = Date.now() - 20_000;
    await write_file(root, '/tree/stale.txt', 'stale', base + 1_000);
    await write_file(root, '/tree/new.txt', 'new', base + 10_000);
    await FileSystem.utimes(Path.join(root, 'tree'), (base + 2_000) / 1_000, (base + 2_000) / 1_000);
    const { manager, map } = await create_map(t, root);

    const applied = await manager.apply_tombstone('/tree', {
        type: 'tombstone',
        target: 'directory',
        deleted_at: base + 5_000,
        include_self: false,
    });
    assert.equal(applied.include_self, false);
    assert.equal((await FileSystem.lstat(Path.join(root, 'tree'))).isDirectory(), true);
    await assert.rejects(FileSystem.access(Path.join(root, 'tree/stale.txt')), { code: 'ENOENT' });
    assert.equal(await FileSystem.readFile(Path.join(root, 'tree/new.txt'), 'utf8'), 'new');
    assert.equal(map.manifest['/tree'].type, 'directory');
    assert.equal(map.get_tombstone('/tree').include_self, false);
    assert.equal(map.canonical_entry('/tree').type, 'directory');
});

test('DirectoryMap rebuilds from disk without persisting private state', async (t) => {
    const root = await temporary_directory(t);
    const ignored_state = Path.join(root, 'obsolete-state');
    await write_file(root, '/offline.txt', 'content');
    let map = new DirectoryMap({ path: root, state: { path: ignored_state }, watcher: WATCHER });
    capture_errors(map);
    await map.ready();
    assert.equal(map.options.state.path, undefined);
    await map.destroy();
    await FileSystem.rm(Path.join(root, 'offline.txt'));

    map = new DirectoryMap({ path: root, watcher: WATCHER });
    const errors = capture_errors(map);
    await map.ready();
    assert.equal(map.manifest['/offline.txt'], undefined);
    assert.equal(map.get_tombstone('/offline.txt'), undefined);
    await assert.rejects(FileSystem.access(Path.join(root, '.directory-sync')), { code: 'ENOENT' });
    await assert.rejects(FileSystem.access(ignored_state), { code: 'ENOENT' });
    assert.deepEqual(errors, []);
    await map.destroy();
});

test('DirectoryMap ignores delayed unlink events after a path has been recreated', async (t) => {
    const root = await temporary_directory(t);
    const path = await write_file(root, '/recreated.txt', 'initial');
    const { errors, map } = await create_map(t, root);
    await write_file(root, '/recreated.txt', 'replacement', Date.now() + 1_000);
    await wait_for(() => map.get_entry('/recreated.txt')?.sha256 === sha256('replacement'));

    map.watcher.emit('unlink', path);
    await wait(100);

    assert.equal(await FileSystem.readFile(path, 'utf8'), 'replacement');
    assert.equal(map.canonical_entry('/recreated.txt').type, 'file');
    assert.equal(map.get_tombstone('/recreated.txt'), undefined);
    assert.deepEqual(errors, []);
});

test('DirectoryMap preserves LTS write stability and Chokidar backend defaults', async (t) => {
    const root = await temporary_directory(t);
    const polling_root = await temporary_directory(t);
    const explicit_root = await temporary_directory(t);
    await write_file(root, '/document.txt', 'initial');
    const editor_tmp = Path.join(root, '.editor-tmp');
    await FileSystem.mkdir(editor_tmp);
    const map = new DirectoryMap({
        path: root,
        watcher: { ignored: (path) => path === editor_tmp || path.startsWith(`${editor_tmp}/`) },
    });
    const errors = capture_errors(map);
    t.after(() => map.destroy());
    await map.ready();

    assert.deepEqual(map.options.watcher.awaitWriteFinish, {
        pollInterval: 100,
        stabilityThreshold: 500,
    });
    assert.equal(map.options.watcher.usePolling, false);
    assert.equal(map.options.watcher.atomic, true);

    const mutations = [];
    map.on('mutation', (uri, entry) => mutations.push({ entry, uri }));
    const temporary = Path.join(editor_tmp, 'editor-atomic-save.tmp');
    await FileSystem.writeFile(temporary, 'atomically-saved');
    await FileSystem.utimes(temporary, (Date.now() + 1_000) / 1_000, (Date.now() + 1_000) / 1_000);
    await FileSystem.rename(temporary, Path.join(root, 'document.txt'));
    await wait_for(() => map.get_entry('/document.txt')?.sha256 === sha256('atomically-saved'));
    await wait(600);

    assert.equal(map.canonical_entry('/document.txt').type, 'file');
    assert.equal(map.get_tombstone('/document.txt'), undefined);
    assert.equal(mutations.some(({ entry }) => entry.type === 'tombstone'), false);
    assert.deepEqual(errors, []);

    const polling = new DirectoryMap({
        path: polling_root,
        watcher: { awaitWriteFinish: false, usePolling: true },
    });
    t.after(() => polling.destroy());
    await polling.ready();
    assert.equal(polling.options.watcher.atomic, false);
    assert.equal(polling.options.watcher.awaitWriteFinish, false);

    const explicit = new DirectoryMap({
        path: explicit_root,
        watcher: {
            atomic: 250,
            awaitWriteFinish: { pollInterval: 20, stabilityThreshold: 333 },
        },
    });
    t.after(() => explicit.destroy());
    await explicit.ready();
    assert.equal(explicit.options.watcher.atomic, 250);
    assert.deepEqual(explicit.options.watcher.awaitWriteFinish, {
        pollInterval: 20,
        stabilityThreshold: 333,
    });
});

test('DirectoryManager blocks writes through symbolic-link parents', async (t) => {
    const root = await temporary_directory(t);
    const outside = await temporary_directory(t, 'directory-sync-outside-');
    await FileSystem.symlink(outside, Path.join(root, 'link'));
    const { manager } = await create_map(t, root);
    const content = Buffer.from('escape');
    await assert.rejects(
        manager.apply_file('/link/escape.txt', content, {
            type: 'file',
            modified_at: Date.now(),
            size: content.length,
            sha256: sha256(content),
        }),
        { code: 'INVALID_URI' }
    );
    await assert.rejects(FileSystem.access(Path.join(outside, 'escape.txt')), { code: 'ENOENT' });
});

test('legacy DirectoryManager convenience methods retain their names and behavior', async (t) => {
    const root = await temporary_directory(t);
    const { manager, map } = await create_map(t, root);

    await manager.create('/directory', true);
    await manager.create('/empty.txt');
    assert.equal(map.get_entry('/directory').type, 'directory');
    assert.equal((await manager.read('/empty.txt')).length, 0);

    await manager.write('/source.txt', 'source');
    assert.deepEqual(await manager.read('/source.txt'), Buffer.from('source'));
    await manager.indirect_write('/buffer.txt', Buffer.from('buffer'));
    assert.equal(await FileSystem.readFile(Path.join(root, 'buffer.txt'), 'utf8'), 'buffer');

    const source = map.get_entry('/source.txt');
    await manager.apply_metadata('/source.txt', { ...source, modified_at: source.modified_at + 1 });
    assert.equal(map.get_entry('/source.txt').modified_at, source.modified_at + 1);
    await manager.move('/source.txt', '/moved.txt');
    assert.equal(await FileSystem.readFile(Path.join(root, 'moved.txt'), 'utf8'), 'source');
    await assert.rejects(FileSystem.access(Path.join(root, 'source.txt')), { code: 'ENOENT' });

    await manager.delete('/buffer.txt');
    assert.equal(map.get_tombstone('/buffer.txt').target, 'file');
    assert.deepEqual(map.supress('/legacy.txt', 'file_create', 2), {
        amount: 2,
        updated_at: map.supressions['file_create:/legacy.txt'].updated_at,
    });
    assert.equal(map._depress('/legacy.txt', 'file_create'), true);
    assert.equal(map.supressions['file_create:/legacy.txt'].amount, 1);
    assert.equal(map._depress('/legacy.txt', 'file_create'), true);
    assert.equal(map._depress('/legacy.txt', 'file_create'), false);
    const legacy_md5 = Crypto.createHash('md5').update('stream').digest('hex');
    await manager.indirect_write('/stream.txt', Readable.from(['stream']), legacy_md5);
    assert.equal(await FileSystem.readFile(Path.join(root, 'stream.txt'), 'utf8'), 'stream');
    assert.equal(map.get_entry('/stream.txt').sha256, sha256('stream'));
});
