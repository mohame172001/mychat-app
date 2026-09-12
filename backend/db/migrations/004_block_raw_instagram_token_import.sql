\set ON_ERROR_STOP on

BEGIN;

COMMENT ON COLUMN mychat.users.meta_access_token IS
    'Legacy source stores raw plaintext. Import is blocked until app-level authenticated encryption is implemented.';
COMMENT ON COLUMN mychat.instagram_accounts.access_token IS
    'Legacy source stores raw plaintext. Import is blocked until app-level authenticated encryption is implemented.';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'users_raw_meta_token_import_blocked'
          AND conrelid = 'mychat.users'::regclass
    ) THEN
        ALTER TABLE mychat.users
            ADD CONSTRAINT users_raw_meta_token_import_blocked
            CHECK (meta_access_token IS NULL) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'instagram_accounts_raw_token_import_blocked'
          AND conrelid = 'mychat.instagram_accounts'::regclass
    ) THEN
        ALTER TABLE mychat.instagram_accounts
            ADD CONSTRAINT instagram_accounts_raw_token_import_blocked
            CHECK (access_token IS NULL) NOT VALID;
    END IF;
END;
$$;

COMMIT;