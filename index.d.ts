import { EventEmitter } from 'node:events';
import type { Stats } from 'node:fs';
import type { Readable } from 'node:stream';
import type { ChokidarOptions, FSWatcher } from 'chokidar';
import type { Server as HyperExpressServer } from 'hyper-express';

export interface FilteringObject {
    files?: string[];
    directories?: string[];
    extensions?: string[];
}

export type FilteringFunction = (uri: string, stats: Stats, strict?: boolean) => boolean;

export interface FileEntry {
    type: 'file';
    modified_at: number;
    size: number;
    sha256: string;
}

export interface DirectoryEntry {
    type: 'directory';
    modified_at: number;
}

export interface TombstoneEntry {
    type: 'tombstone';
    target: 'file' | 'directory';
    deleted_at: number;
    include_self?: boolean;
}

export type Entry = FileEntry | DirectoryEntry | TombstoneEntry;

export interface StateOptions {
    /** State directory. Defaults to `.directory-sync` inside the synchronized root. */
    path?: string;
    /** Maximum retained tombstones. Oldest insertion is evicted first. Defaults to 10,000. */
    max_tombstones?: number;
}

export interface HashingOptions {
    /** Maximum files hashed concurrently. Defaults to at most four logical CPUs. */
    max_concurrent?: number;
}

export interface DirectoryMapOptions {
    path?: string;
    filters?: {
        keep?: FilteringObject | FilteringFunction;
        ignore?: FilteringObject | FilteringFunction;
    };
    watcher?: ChokidarOptions;
    limits?: { max_file_size?: number };
    state?: StateOptions;
    hashing?: HashingOptions;
}

export interface MapRecord {
    uri: string;
    path: string;
    entry: FileEntry | DirectoryEntry;
    stats: {
        md5: string;
        sha256: string;
        size: number;
        created_at: number;
        modified_at: number;
    };
}

/** Runtime map exposed through `Mirror.map` and `Server.hosts[name].map`. */
export interface DirectoryMap {
    relative_uri(path: string): string;
    resolve(uri: string): string;
    allows(uri: string, type?: 'file' | 'directory'): boolean;
    expect(uri: string, entry: Entry, ttl?: number): void;
    /** Backward-compatible misspelling retained from v2. */
    supress(uri: string, event?: string, amount?: number): { amount: number; updated_at: number } | undefined;
    /** Backward-compatible suppression cleanup helper. */
    _depress(uri: string, event?: string, amount?: number): boolean;
    _cleanup_supressions(max_age_ms?: number): void;
    commit_entry(uri: string, entry: FileEntry | DirectoryEntry, stats?: Stats): MapRecord;
    commit_tombstone(uri: string, entry: TombstoneEntry): Promise<TombstoneEntry>;
    active_entries_under(uri: string): Record<string, FileEntry | DirectoryEntry>;
    run_serial<T>(uri: string, handler: () => T | Promise<T>): Promise<T>;
    get(uri: string): MapRecord | undefined;
    get_entry(uri: string): FileEntry | DirectoryEntry | undefined;
    get_tombstone(uri: string): TombstoneEntry | undefined;
    canonical_entry(uri: string): Entry | undefined;
    ready(): Promise<void>;
    destroy(): Promise<void>;
    readonly destroyed: boolean;
    readonly path: string;
    readonly watcher: FSWatcher;
    readonly options: DirectoryMapOptions;
    readonly directories: Record<string, MapRecord>;
    readonly files: Record<string, MapRecord>;
    readonly supressions: Record<string, { amount: number; updated_at: number }>;
    readonly manifest: Record<string, Entry>;
    readonly schema: Record<string, Array<string | number>>;
    readonly serializable_options: Omit<DirectoryMapOptions, 'path'>;
    readonly state: StateStore;
}

export interface StateStore {
    initialize(): Promise<void>;
    get_cached(uri: string): FileEntry | DirectoryEntry | undefined;
    get_tombstone(uri: string): TombstoneEntry | undefined;
    effective_tombstone(uri: string): TombstoneEntry | undefined;
    set_active(uri: string, entry: FileEntry | DirectoryEntry): void;
    remove_active(uri: string): void;
    remove_active_tree(uri: string): void;
    replace_active(entries: Iterable<[string, FileEntry | DirectoryEntry]>): void;
    record_tombstone(uri: string, entry: TombstoneEntry): Promise<TombstoneEntry>;
    schedule_snapshot(): void;
    compact(): Promise<void>;
    close(): Promise<void>;
    readonly active: Map<string, FileEntry | DirectoryEntry>;
    readonly tombstones: Map<string, TombstoneEntry>;
    readonly path: string;
    readonly tmp_path: string;
}

export interface ApplyFileOptions {
    signal?: AbortSignal;
    supress?: boolean;
    validate_before_commit?: () => boolean | Promise<boolean>;
}

export interface DirectoryManager {
    _absolute_path(uri: string): string;
    read(uri: string, stream: true): Readable;
    read(uri: string, stream?: false): Promise<Buffer>;
    apply_file(
        uri: string,
        data: string | Buffer | ArrayBuffer | ArrayBufferView | NodeJS.ReadableStream | ReadableStream,
        entry: FileEntry,
        options?: ApplyFileOptions,
    ): Promise<MapRecord>;
    apply_metadata(uri: string, entry: FileEntry): Promise<MapRecord>;
    apply_directory(uri: string, entry: DirectoryEntry): Promise<MapRecord>;
    apply_tombstone(uri: string, entry: TombstoneEntry): Promise<TombstoneEntry>;
    create(uri: string, is_directory?: boolean): Promise<MapRecord | undefined>;
    write(
        uri: string,
        data: string | Buffer | ArrayBuffer | ArrayBufferView | NodeJS.ReadableStream,
        sha256?: string,
        supress?: boolean,
    ): Promise<MapRecord>;
    indirect_write(
        uri: string,
        data: string | Buffer | NodeJS.ReadableStream,
        sha256?: string,
        supress?: boolean,
    ): Promise<MapRecord>;
    move(from_uri: string, to_uri: string, supress?: boolean): Promise<TombstoneEntry | undefined>;
    delete(uri: string, is_directory?: boolean, supress?: boolean): Promise<TombstoneEntry | undefined>;
}

export interface HostedDirectory {
    path: string;
    map: DirectoryMap;
    manager: DirectoryManager;
}

export interface ServerOptions {
    port?: number;
    auth?: string;
    ssl?: {
        key?: string;
        cert?: string;
        passphrase?: string;
        dh_params?: string;
        prefer_low_memory_usage?: boolean;
    };
    limits?: {
        max_body_length?: number;
        fast_buffers?: boolean;
    };
}

export interface MirrorOptions extends DirectoryMapOptions {
    path: string;
    hostname: string;
    port?: number;
    ssl?: boolean;
    auth?: string;
    retry?: { every?: number; backoff?: boolean };
    queue?: {
        max_concurrent?: number;
        max_queued?: number;
        timeout?: number;
        throttle?: { rate?: number; interval?: number };
    };
}

/** Central authority that hosts one or more named synchronized repositories. */
export class Server extends EventEmitter {
    constructor(options?: ServerOptions);

    /** Starts tracking a named repository and makes it available to mirrors. */
    host(name: string, path: string, options?: DirectoryMapOptions): Promise<HostedDirectory>;

    /** Stops hosting a repository. Returns false when the name was not hosted. */
    unhost(name: string): Promise<boolean>;

    /** Resolves after HyperExpress has bound its listening socket. */
    ready(): Promise<this>;

    /** Gracefully closes HTTP/WebSocket resources and every hosted map. */
    destroy(): Promise<void>;

    readonly hosts: Record<string, HostedDirectory>;
    readonly server: HyperExpressServer;
    readonly options: ServerOptions;
    readonly destroyed: boolean;
}

/** Bidirectional node that pushes local mutations to, and pulls accepted state from, a Server. */
export class Mirror extends EventEmitter {
    constructor(host: string, options: MirrorOptions);

    /** Resolves after the initial manifest reconciliation has completed. */
    ready(): Promise<this>;

    /** Stops transfers, leaves the shared event connection, and closes the local map. */
    destroy(): Promise<void>;

    readonly host: string;
    readonly destroyed: boolean;
    readonly options: MirrorOptions;
    readonly map: DirectoryMap;
}
