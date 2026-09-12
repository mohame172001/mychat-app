\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE mychat.users
    ADD COLUMN IF NOT EXISTS fb_page_access_token text;

ALTER TABLE mychat.users
    DROP CONSTRAINT IF EXISTS users_raw_meta_token_import_blocked;

ALTER TABLE mychat.users
    ADD CONSTRAINT users_encrypted_meta_token_only
    CHECK (
        meta_access_token IS NULL
        OR meta_access_token LIKE 'mychat:ig-token:v1:%'
    ) NOT VALID;

ALTER TABLE mychat.users
    ADD CONSTRAINT users_encrypted_page_token_only
    CHECK (
        fb_page_access_token IS NULL
        OR fb_page_access_token LIKE 'mychat:ig-token:v1:%'
    ) NOT VALID;

COMMENT ON COLUMN mychat.users.meta_access_token IS
    'Runtime writes must use a mychat:ig-token:v1 authenticated-encryption envelope; raw source import remains blocked.';
COMMENT ON COLUMN mychat.users.fb_page_access_token IS
    'Runtime writes must use a mychat:ig-token:v1 authenticated-encryption envelope; raw source import remains blocked.';

COMMIT;