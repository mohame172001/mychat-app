\set ON_ERROR_STOP on

BEGIN;

CREATE OR REPLACE FUNCTION mychat.jsonb_contains_token_key(value jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    item_key text;
    item_value jsonb;
BEGIN
    IF jsonb_typeof(value) = 'object' THEN
        FOR item_key, item_value IN SELECT * FROM jsonb_each(value)
        LOOP
            IF regexp_replace(lower(item_key), '[^a-z0-9]', '', 'g') IN (
                'token',
                'authorization',
                'accesstoken',
                'metaaccesstoken',
                'fbpageaccesstoken',
                'pageaccesstoken',
                'longlivedaccesstoken',
                'refreshtoken'
            ) OR regexp_replace(lower(item_key), '[^a-z0-9]', '', 'g')
                ~ '(access|refresh)token$'
            THEN
                RETURN true;
            END IF;
            IF mychat.jsonb_contains_token_key(item_value) THEN
                RETURN true;
            END IF;
        END LOOP;
    ELSIF jsonb_typeof(value) = 'array' THEN
        FOR item_value IN SELECT * FROM jsonb_array_elements(value)
        LOOP
            IF mychat.jsonb_contains_token_key(item_value) THEN
                RETURN true;
            END IF;
        END LOOP;
    END IF;
    RETURN false;
END;
$$;

ALTER TABLE mychat.users
    ADD CONSTRAINT users_profile_has_no_token_keys
    CHECK (NOT mychat.jsonb_contains_token_key(profile)) NOT VALID;

ALTER TABLE mychat.users
    ADD CONSTRAINT users_source_extra_has_no_token_keys
    CHECK (NOT mychat.jsonb_contains_token_key(source_extra)) NOT VALID;

COMMENT ON FUNCTION mychat.jsonb_contains_token_key(jsonb) IS
    'Rejects token-shaped keys recursively so credentials cannot bypass typed encrypted columns through JSONB.';

COMMIT;