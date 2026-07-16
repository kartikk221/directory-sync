import assert from 'node:assert/strict';
import FileSystem from 'node:fs/promises';
import Path from 'node:path';
import test from 'node:test';
import {
    ascii_to_hex,
    async_wait,
    canonicalize_uri,
    compare_entries,
    constant_time_equal,
    entries_equivalent,
    generate_sha256_hash,
    hex_to_ascii,
    is_accessible_path,
    match_extension,
    merge_options,
    resolve_uri,
    safe_json_parse,
    to_path_uri,
    validate_entry,
} from '../src/utils/operators.js';
import { temporary_directory, write_file } from './helpers.js';

const FILE = {
    type: 'file',
    modified_at: 1_000,
    size: 3,
    sha256: 'a'.repeat(64),
};

test('merge_options deeply preserves defaults and caller-owned values', () => {
    const defaults = { nested: { first: 1, values: ['a'] }, enabled: true };
    const provided = { nested: { second: 2, values: ['b'] } };
    const merged = merge_options(defaults, provided);

    assert.deepEqual(merged, {
        nested: { first: 1, second: 2, values: ['b'] },
        enabled: true,
    });
    merged.nested.values.push('c');
    assert.deepEqual(defaults.nested.values, ['a']);
    assert.deepEqual(provided.nested.values, ['b']);
    assert.throws(() => merge_options([], {}), TypeError);
});

test('canonical URIs reject traversal, reserved state, and ambiguous paths', () => {
    assert.equal(canonicalize_uri('nested/file.txt'), '/nested/file.txt');
    assert.equal(canonicalize_uri('//nested///file.txt/'), '/nested/file.txt');
    for (const uri of ['/', '/../secret', '/a/./b', '/a\\b', '/.directory-sync/state', '/a\0b'])
        assert.throws(() => canonicalize_uri(uri));
    assert.equal(canonicalize_uri('/', { allow_root: true }), '/');
});

test('resolve_uri cannot escape the synchronized root', async (t) => {
    const root = await temporary_directory(t);
    assert.equal(resolve_uri(root, '/nested/file'), Path.join(root, 'nested/file'));
    assert.throws(() => resolve_uri(root, '/../outside'));
});

test('entry comparison is newest-wins with authority winning exact ties', () => {
    const newer = { ...FILE, modified_at: 2_000, sha256: 'b'.repeat(64) };
    const tie = { ...FILE, sha256: 'c'.repeat(64) };
    const deletion = { type: 'tombstone', target: 'file', deleted_at: 3_000 };

    assert.equal(compare_entries(FILE, newer), 1);
    assert.equal(compare_entries(newer, FILE), -1);
    assert.equal(compare_entries(FILE, tie), -1);
    assert.equal(compare_entries(FILE, deletion), 1);
    assert.equal(compare_entries(FILE, { ...FILE }), 0);
    assert.equal(entries_equivalent(FILE, { ...FILE }), true);
});

test('entry validation rejects malformed wire data', () => {
    assert.equal(validate_entry(FILE), FILE);
    assert.throws(() => validate_entry({ ...FILE, modified_at: 1.5 }), /modified_at/);
    assert.throws(() => validate_entry({ ...FILE, sha256: 'invalid' }), /sha256/);
    assert.throws(() => validate_entry({ type: 'tombstone', target: 'link', deleted_at: 1 }), /target/);
    assert.throws(() => validate_entry({ type: 'unknown' }), /Unsupported/);
});

test('hashing streams files and authentication comparison handles arbitrary values', async (t) => {
    const root = await temporary_directory(t);
    const path = await write_file(root, '/content.bin', Buffer.from([0, 1, 2, 3]));
    assert.equal(
        await generate_sha256_hash(path),
        '054edec1d0211f624fed0cbca9d4f9400b0e491c43742af2c5b0abebf0c990d8'
    );
    assert.equal(constant_time_equal('secret', 'secret'), true);
    assert.equal(constant_time_equal('secret', 'different'), false);
    await FileSystem.rm(path);
});

test('legacy utility helpers preserve path, encoding, parsing, and wait behavior', async (t) => {
    assert.equal(to_path_uri('nested\\file.txt'), '/nested/file.txt');
    assert.equal(match_extension('index.js', 'js'), true);
    assert.equal(match_extension('index.json', '.js'), false);
    const encoded = ascii_to_hex('repository');
    assert.equal(hex_to_ascii(encoded), 'repository');
    assert.deepEqual(safe_json_parse('{"valid":true}'), { valid: true });
    assert.equal(safe_json_parse('invalid', false), false);

    const root = await temporary_directory(t);
    assert.equal(await is_accessible_path(root, { directory: true }), true);
    assert.equal(await is_accessible_path(Path.join(root, 'missing')), false);
    await async_wait(1);
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(async_wait(1, controller.signal), { name: 'AbortError' });
});
