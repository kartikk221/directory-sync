import assert from 'node:assert/strict';
import test from 'node:test';
import StateStore from '../src/components/directory/StateStore.js';
import { temporary_directory } from './helpers.js';

function tombstone(deleted_at, target = 'file') {
    return { type: 'tombstone', target, deleted_at, include_self: true };
}

test('StateStore persists tombstones and evicts the oldest insertion', async (t) => {
    const root = await temporary_directory(t);
    let store = new StateStore(root, { max_tombstones: 2 });
    await store.initialize();
    await store.record_tombstone('/first', tombstone(1));
    await store.record_tombstone('/second', tombstone(2));
    await store.record_tombstone('/third', tombstone(3));

    assert.equal(store.get_tombstone('/first'), undefined);
    assert.equal(store.tombstones.size, 2);
    await store.close();

    store = new StateStore(root, { max_tombstones: 2 });
    await store.initialize();
    assert.equal(store.get_tombstone('/first'), undefined);
    assert.equal(store.get_tombstone('/second').deleted_at, 2);
    assert.equal(store.get_tombstone('/third').deleted_at, 3);
    await store.close();
});

test('StateStore compacts descendant deletions under a directory tombstone', async (t) => {
    const root = await temporary_directory(t);
    const store = new StateStore(root);
    await store.initialize();
    await store.record_tombstone('/tree/child', tombstone(10));
    await store.record_tombstone('/tree/other', tombstone(11));
    await store.record_tombstone('/tree', tombstone(12, 'directory'));

    assert.equal(store.tombstones.size, 1);
    assert.equal(store.effective_tombstone('/tree/child/deep').deleted_at, 12);
    await store.close();
});

test('StateStore rejects stale replacement tombstones and retains deletion history on resurrection', async (t) => {
    const root = await temporary_directory(t);
    const store = new StateStore(root);
    await store.initialize();
    await store.record_tombstone('/tree', {
        ...tombstone(100, 'directory'),
        include_self: false,
    });
    const retained = await store.record_tombstone('/tree', tombstone(90, 'directory'));
    assert.equal(retained.deleted_at, 100);

    store.set_active('/tree', { type: 'directory', modified_at: 101 });
    assert.equal(store.get_tombstone('/tree').include_self, false);
    assert.equal(store.effective_tombstone('/tree'), undefined);
    assert.equal(store.effective_tombstone('/tree/old').deleted_at, 100);

    await store.record_tombstone('/file', tombstone(100));
    store.set_active('/file', {
        type: 'file',
        modified_at: 101,
        size: 0,
        sha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    });
    assert.equal(store.get_tombstone('/file'), undefined);
    await store.close();
});

test('StateStore tolerates a truncated journal tail', async (t) => {
    const root = await temporary_directory(t);
    const store = new StateStore(root);
    await store.initialize();
    await store.record_tombstone('/valid', tombstone(10));
    await store.close();

    const FileSystem = await import('node:fs/promises');
    await FileSystem.appendFile(`${root}/.directory-sync/tombstones.ndjson`, '{"op":');
    const logs = [];
    const reopened = new StateStore(root, {}, (code) => logs.push(code));
    await reopened.initialize();
    assert.equal(reopened.get_tombstone('/valid').deleted_at, 10);
    assert.ok(logs.includes('STATE_REBUILT'));
    await reopened.close();
});
