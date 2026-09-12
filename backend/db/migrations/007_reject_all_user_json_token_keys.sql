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
    normalized_key text;
BEGIN
    IF jsonb_typeof(value) = 'object' THEN
        FOR item_key, item_value IN SELECT * FROM jsonb_each(value)
        LOOP
            normalized_key := regexp_replace(
                lower(item_key),
                '[^a-z0-9]',
                '',
                'g'
            );
            IF normalized_key = 'authorization'
                OR normalized_key ~ 'token$'
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

COMMENT ON FUNCTION mychat.jsonb_contains_token_key(jsonb) IS
    'Rejects authorization and every token-suffixed key recursively so credentials cannot bypass typed encrypted columns through JSONB.';

COMMIT;