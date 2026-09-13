\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE mychat.instagram_accounts
    DROP CONSTRAINT IF EXISTS instagram_accounts_raw_token_import_blocked;
ALTER TABLE mychat.instagram_accounts
    ADD CONSTRAINT instagram_accounts_encrypted_access_token_only
    CHECK (access_token IS NULL OR access_token ~ '^mychat:ig-token:v1:[A-Za-z0-9_-]{98,}={0,2}$'),
    ADD CONSTRAINT instagram_accounts_encrypted_refresh_token_only
    CHECK (refresh_token IS NULL OR refresh_token ~ '^mychat:ig-token:v1:[A-Za-z0-9_-]{98,}={0,2}$'),
    ADD CONSTRAINT instagram_accounts_provider_metadata_has_no_token_keys
    CHECK (NOT mychat.jsonb_contains_token_key(provider_metadata)),
    ADD CONSTRAINT instagram_accounts_source_extra_has_no_token_keys
    CHECK (NOT mychat.jsonb_contains_token_key(source_extra));

COMMIT;
