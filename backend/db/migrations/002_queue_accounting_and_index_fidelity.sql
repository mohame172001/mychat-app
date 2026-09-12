\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE mychat.schema_migrations
    ADD COLUMN IF NOT EXISTS checksum_sha256 text;

ALTER TABLE mychat.usage_reservations
    ADD COLUMN IF NOT EXISTS reservation_id text,
    ADD COLUMN IF NOT EXISTS instagram_account_id text,
    ADD COLUMN IF NOT EXISTS automation_id text,
    ADD COLUMN IF NOT EXISTS ig_comment_id text,
    ADD COLUMN IF NOT EXISTS action_id text,
    ADD COLUMN IF NOT EXISTS event_type text,
    ADD COLUMN IF NOT EXISTS source text,
    ADD COLUMN IF NOT EXISTS reserved_at timestamptz,
    ADD COLUMN IF NOT EXISTS confirmed_at timestamptz,
    ADD COLUMN IF NOT EXISTS released_at timestamptz,
    ADD COLUMN IF NOT EXISTS failure_reason_sanitized text;

ALTER TABLE mychat.usage_reservation_buckets
    ADD COLUMN IF NOT EXISTS user_id text,
    ADD COLUMN IF NOT EXISTS instagram_account_id text,
    ADD COLUMN IF NOT EXISTS confirmed_amount bigint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS created_at timestamptz;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'usage_buckets_confirmed_nonnegative'
          AND conrelid = 'mychat.usage_reservation_buckets'::regclass
    ) THEN
        ALTER TABLE mychat.usage_reservation_buckets
            ADD CONSTRAINT usage_buckets_confirmed_nonnegative
            CHECK (confirmed_amount >= 0);
    END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS usage_reservations_reservation_id_uq
    ON mychat.usage_reservations (reservation_id)
    WHERE reservation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS usage_reservations_account_idx
    ON mychat.usage_reservations (instagram_account_id)
    WHERE instagram_account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS usage_reservations_automation_idx
    ON mychat.usage_reservations (automation_id)
    WHERE automation_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS usage_reservation_buckets_user_idx
    ON mychat.usage_reservation_buckets (user_id)
    WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS usage_reservation_buckets_account_idx
    ON mychat.usage_reservation_buckets (instagram_account_id)
    WHERE instagram_account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS instagram_trial_claim_claimant_created_idx
    ON mychat.instagram_account_trial_claims
    (first_claimed_by_user_id, claimed_at DESC);
CREATE INDEX IF NOT EXISTS instagram_accounts_external_idx
    ON mychat.instagram_accounts (instagram_account_id)
    WHERE instagram_account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS instagram_events_created_idx
    ON mychat.instagram_automation_events (created_at DESC);
CREATE INDEX IF NOT EXISTS user_limit_overrides_creator_created_idx
    ON mychat.user_limit_overrides (created_by_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS admin_members_disabled_idx
    ON mychat.admin_members (disabled_at);

-- Explicit supporting indexes for nullable foreign keys. These preserve import
-- flexibility until production profiling determines which constraints can be
-- tightened.
CREATE INDEX IF NOT EXISTS automations_user_fk_idx ON mychat.automations (user_id);
CREATE INDEX IF NOT EXISTS automations_account_fk_idx ON mychat.automations (instagram_account_id);
CREATE INDEX IF NOT EXISTS comments_user_fk_idx ON mychat.comments (user_id);
CREATE INDEX IF NOT EXISTS comments_account_fk_idx ON mychat.comments (instagram_account_id);
CREATE INDEX IF NOT EXISTS comments_automation_fk_idx ON mychat.comments (automation_id);
CREATE INDEX IF NOT EXISTS comments_contact_fk_idx ON mychat.comments (contact_id);
CREATE INDEX IF NOT EXISTS comment_sessions_rule_fk_idx ON mychat.comment_dm_sessions (rule_id);
CREATE INDEX IF NOT EXISTS dm_logs_conversation_fk_idx ON mychat.dm_logs (conversation_id);
CREATE INDEX IF NOT EXISTS dm_logs_contact_fk_idx ON mychat.dm_logs (contact_id);
CREATE INDEX IF NOT EXISTS conversation_messages_conversation_fk_idx
    ON mychat.conversation_messages (conversation_id);
CREATE INDEX IF NOT EXISTS broadcasts_user_fk_idx ON mychat.broadcasts (user_id);
CREATE INDEX IF NOT EXISTS broadcasts_account_fk_idx ON mychat.broadcasts (instagram_account_id);
CREATE INDEX IF NOT EXISTS subscriptions_user_fk_idx ON mychat.subscriptions (user_id);
CREATE INDEX IF NOT EXISTS invoices_user_fk_idx ON mychat.invoices (user_id);

CREATE OR REPLACE FUNCTION mychat.claim_webhook_job(
    p_owner text,
    p_fencing_token text,
    p_lease interval DEFAULT interval '180 seconds'
)
RETURNS SETOF mychat.webhook_inbox
LANGUAGE plpgsql
AS $$
DECLARE
    claimed mychat.webhook_inbox%ROWTYPE;
BEGIN
    IF p_owner IS NULL OR p_owner = '' OR p_fencing_token IS NULL OR p_fencing_token = '' THEN
        RAISE EXCEPTION 'owner and fencing token are required';
    END IF;
    IF p_lease <= interval '0 seconds' THEN
        RAISE EXCEPTION 'lease must be positive';
    END IF;

    -- A worker that died after taking the fifth attempt cannot report failure.
    -- Terminalize that expired lease before looking for another eligible job.
    UPDATE mychat.webhook_inbox
    SET status = 'failed',
        payload_encrypted = NULL,
        owner = NULL,
        fencing_token = NULL,
        lease_until = NULL,
        expires_at = clock_timestamp() + interval '30 days',
        last_error = COALESCE(last_error, 'attempts_exhausted_after_lease_expiry'),
        updated_at = clock_timestamp()
    WHERE status = 'processing'
      AND attempts >= 5
      AND lease_until <= clock_timestamp();

    WITH candidate AS (
        SELECT event_digest
        FROM mychat.webhook_inbox
        WHERE attempts < 5
          AND available_at <= clock_timestamp()
          AND (
              status = 'pending'
              OR (status = 'processing' AND lease_until <= clock_timestamp())
          )
        ORDER BY available_at, event_digest
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    UPDATE mychat.webhook_inbox AS job
    SET status = 'processing',
        owner = p_owner,
        fencing_token = p_fencing_token,
        lease_until = clock_timestamp() + p_lease,
        attempts = job.attempts + 1,
        updated_at = clock_timestamp()
    FROM candidate
    WHERE job.event_digest = candidate.event_digest
    RETURNING job.* INTO claimed;

    IF FOUND THEN
        RETURN NEXT claimed;
    END IF;
    RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION mychat.complete_webhook_job(
    p_event_digest text,
    p_fencing_token text
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer;
BEGIN
    UPDATE mychat.webhook_inbox
    SET status = 'completed',
        payload_encrypted = NULL,
        owner = NULL,
        fencing_token = NULL,
        lease_until = NULL,
        expires_at = clock_timestamp() + interval '7 days',
        last_error = NULL,
        updated_at = clock_timestamp()
    WHERE event_digest = p_event_digest
      AND status = 'processing'
      AND fencing_token = p_fencing_token;
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected = 1;
END;
$$;

CREATE OR REPLACE FUNCTION mychat.fail_webhook_job(
    p_event_digest text,
    p_fencing_token text,
    p_error text,
    p_retry_delay interval DEFAULT interval '1 minute'
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    affected integer;
BEGIN
    IF p_retry_delay < interval '0 seconds' THEN
        RAISE EXCEPTION 'retry delay cannot be negative';
    END IF;

    UPDATE mychat.webhook_inbox
    SET status = CASE WHEN attempts >= 5 THEN 'failed' ELSE 'pending' END,
        payload_encrypted = CASE WHEN attempts >= 5 THEN NULL ELSE payload_encrypted END,
        available_at = CASE
            WHEN attempts >= 5 THEN available_at
            ELSE clock_timestamp() + p_retry_delay
        END,
        owner = NULL,
        fencing_token = NULL,
        lease_until = NULL,
        expires_at = CASE
            WHEN attempts >= 5 THEN clock_timestamp() + interval '30 days'
            ELSE expires_at
        END,
        last_error = left(COALESCE(p_error, 'processing_failed'), 500),
        updated_at = clock_timestamp()
    WHERE event_digest = p_event_digest
      AND status = 'processing'
      AND fencing_token = p_fencing_token;
    GET DIAGNOSTICS affected = ROW_COUNT;
    RETURN affected = 1;
END;
$$;

COMMIT;