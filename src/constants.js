const PROTOCOL_VERSION = 5;
const PROTOCOL_PATH = `/v${PROTOCOL_VERSION}`;
const PROTOCOL_HEADER = 'x-directory-sync-protocol';
const AUTH_HEADER = 'x-auth-key';
const ACTOR_HEADER = 'x-directory-sync-actor';
const MODIFIED_HEADER = 'x-directory-sync-modified-at';
const HASH_HEADER = 'x-directory-sync-sha256';
const EPOCH_HEADER = 'x-directory-sync-epoch';
const BASE_REVISION_HEADER = 'x-directory-sync-base-revision';
const REVISION_HEADER = 'x-directory-sync-revision';

export {
    ACTOR_HEADER,
    AUTH_HEADER,
    BASE_REVISION_HEADER,
    EPOCH_HEADER,
    HASH_HEADER,
    MODIFIED_HEADER,
    PROTOCOL_HEADER,
    PROTOCOL_PATH,
    PROTOCOL_VERSION,
    REVISION_HEADER,
};
