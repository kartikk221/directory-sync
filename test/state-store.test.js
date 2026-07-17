import assert from 'node:assert/strict';
import FileSystem from 'node:fs/promises';
import Path from 'node:path';
import test from 'node:test';
import StateStore from '../src/components/directory/StateStore.js';
import { temporary_directory } from './helpers.js';

function tombstone(deleted_at, target = 'file') {
    return { type: 'tombstone', target, deleted_at, include_self: true };
}

test('StateStore keeps bounded tombstones only in memory', async (t) => {
    const root = await temporary_directory(t);
    let store = new StateStore({ max_tombstones: 2 });
    await store.record_tombstone('/first', tombstone(1));
    await store.record_tombstone('/second', tombstone(2));
    await store.record_tombstone('/third', tombstone(3));

    assert.equal(store.get_tombstone('/first'), undefined);
    assert.equal(store.tombstones.size, 2);

    store = new StateStore({ max_tombstones: 2 });
    assert.equal(store.tombstones.size, 0);
    await assert.rejects(FileSystem.access(Path.join(root, '.directory-sync')), { code: 'ENOENT' });
});

test('StateStore removes only descendant tombstones covered by a newer directory deletion', async () => {
    const store = new StateStore();
    await store.record_tombstone('/tree/older', tombstone(10));
    await store.record_tombstone('/tree/newer', tombstone(30));
    await store.record_tombstone('/tree', tombstone(20, 'directory'));

    assert.equal(store.get_tombstone('/tree/older'), undefined);
    assert.equal(store.get_tombstone('/tree/newer').deleted_at, 30);
    assert.equal(store.effective_tombstone('/tree/child').deleted_at, 20);
});

test('StateStore retains distinct file and directory deletion history at one path', async () => {
    const store = new StateStore();
    await store.record_tombstone('/entry', tombstone(100, 'directory'));
    await store.record_tombstone('/entry', tombstone(200, 'file'));

    assert.equal(store.get_tombstone('/entry', 'directory').deleted_at, 100);
    assert.equal(store.get_tombstone('/entry', 'file').deleted_at, 200);
    assert.equal(store.records.length, 2);
});

test('StateStore keeps tombstones when a newer active entry is recorded', async () => {
    const store = new StateStore();
    await store.record_tombstone('/file', tombstone(100));
    store.set_active('/file', {
        type: 'file',
        modified_at: 101,
        size: 0,
        sha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    });

    assert.equal(store.get_tombstone('/file').deleted_at, 100);
    assert.equal(store.active.get('/file').modified_at, 101);
});
