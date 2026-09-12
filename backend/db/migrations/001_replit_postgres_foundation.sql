\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA IF NOT EXISTS mychat;

CREATE TABLE IF NOT EXISTS mychat.schema_migrations (
    version text PRIMARY KEY,
    description text NOT NULL,
    checksum_sha256 text,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS mychat.users (
    id text PRIMARY KEY,
    normalized_email text,
    email text,
    username text,
    status text,
    google_sub text,
    password_hash text,
    email_verification_token_hash text,
    email_verification_expires_at timestamptz,
    password_reset_token_hash text,
    password_reset_expires_at timestamptz,
    meta_access_token text,
    created_at timestamptz,
    updated_at timestamptz,
    profile jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.instagram_accounts (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text,
    legacy_user_id text,
    ig_user_id text,
    username text,
    is_active boolean,
    connection_valid boolean,
    refresh_status text,
    token_expires_at timestamptz,
    access_token text,
    refresh_token text,
    created_at timestamptz,
    updated_at timestamptz,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.instagram_account_trial_claims (
    id text PRIMARY KEY,
    instagram_account_id text,
    instagram_account_row_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    plan_trial_identifier text NOT NULL,
    first_claimed_by_user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    claimed_at timestamptz,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.automations (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    name text,
    status text,
    automation_type text,
    trigger_type text,
    run_count bigint NOT NULL DEFAULT 0 CHECK (run_count >= 0),
    created_at timestamptz,
    updated_at timestamptz,
    nodes jsonb NOT NULL DEFAULT '[]'::jsonb,
    edges jsonb NOT NULL DEFAULT '[]'::jsonb,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.dm_rules (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    automation_id text REFERENCES mychat.automations(id) ON DELETE SET NULL,
    is_active boolean,
    match_type text,
    created_at timestamptz,
    updated_at timestamptz,
    rule_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.contacts (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    instagram_user_id text,
    username text,
    is_subscribed boolean,
    last_activity_at timestamptz,
    created_at timestamptz,
    updated_at timestamptz,
    tags jsonb NOT NULL DEFAULT '[]'::jsonb,
    profile jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.conversations (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    contact_id text REFERENCES mychat.contacts(id) ON DELETE SET NULL,
    status text,
    unread_count bigint NOT NULL DEFAULT 0 CHECK (unread_count >= 0),
    last_message_text text,
    last_message_at timestamptz,
    created_at timestamptz,
    updated_at timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.conversation_messages (
    id text PRIMARY KEY,
    conversation_id text REFERENCES mychat.conversations(id) ON DELETE CASCADE,
    source_ordinal integer CHECK (source_ordinal IS NULL OR source_ordinal >= 0),
    direction text,
    status text,
    message_text text,
    sent_at timestamptz,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.broadcasts (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    name text,
    status text,
    scheduled_at timestamptz,
    sent_count bigint NOT NULL DEFAULT 0 CHECK (sent_count >= 0),
    failed_count bigint NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
    created_at timestamptz,
    updated_at timestamptz,
    audience_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    message_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.instagram_media_catalog (
    id text PRIMARY KEY,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    media_id text NOT NULL,
    media_type text,
    media_timestamp timestamptz,
    caption text,
    permalink text,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.comments (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    automation_id text REFERENCES mychat.automations(id) ON DELETE SET NULL,
    contact_id text REFERENCES mychat.contacts(id) ON DELETE SET NULL,
    ig_comment_id text,
    media_id text,
    instagram_author_id text,
    comment_text text,
    action_status text,
    reply_status text,
    dm_status text,
    media_ownership_check text,
    opening_dedupe_key text,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at timestamptz,
    queue_lock_until timestamptz,
    queue_owner text,
    fencing_token text,
    created_at timestamptz,
    updated_at timestamptz,
    decision_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.comment_dm_sessions (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    recipient_id text,
    rule_id text REFERENCES mychat.dm_rules(id) ON DELETE SET NULL,
    media_id text,
    status text,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    opening_dedupe_key text,
    expires_at timestamptz,
    created_at timestamptz,
    updated_at timestamptz,
    gate_state jsonb NOT NULL DEFAULT '{}'::jsonb,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.dm_logs (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    conversation_id text REFERENCES mychat.conversations(id) ON DELETE SET NULL,
    contact_id text REFERENCES mychat.contacts(id) ON DELETE SET NULL,
    direction text,
    status text,
    provider_message_id text,
    dedup_key text,
    message_text text,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at timestamptz,
    created_at timestamptz,
    updated_at timestamptz,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.tracked_links (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    rule_id text REFERENCES mychat.dm_rules(id) ON DELETE SET NULL,
    short_code text,
    target_url text,
    click_count bigint NOT NULL DEFAULT 0 CHECK (click_count >= 0),
    expires_at timestamptz,
    created_at timestamptz,
    updated_at timestamptz,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.link_click_events (
    id text PRIMARY KEY,
    tracked_link_id text REFERENCES mychat.tracked_links(id) ON DELETE SET NULL,
    short_code text,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    instagram_user_id text,
    clicked_at timestamptz NOT NULL,
    expires_at timestamptz,
    request_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.usage_events (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    automation_id text REFERENCES mychat.automations(id) ON DELETE SET NULL,
    event_type text NOT NULL,
    event_date date,
    event_month text NOT NULL,
    limit_subject_type text,
    limit_subject_id text,
    amount bigint NOT NULL DEFAULT 1 CHECK (amount >= 0),
    created_at timestamptz NOT NULL,
    attribution jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (event_month ~ '^[0-9]{4}-[0-9]{2}$')
);

CREATE TABLE IF NOT EXISTS mychat.monthly_usage (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    event_month text NOT NULL,
    limit_subject_type text,
    limit_subject_id text,
    metric text,
    used_amount bigint NOT NULL DEFAULT 0 CHECK (used_amount >= 0),
    created_at timestamptz,
    updated_at timestamptz,
    counters jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (event_month ~ '^[0-9]{4}-[0-9]{2}$')
);

CREATE TABLE IF NOT EXISTS mychat.usage_reservations (
    id text PRIMARY KEY,
    idempotency_key text NOT NULL,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    limit_subject_type text NOT NULL,
    limit_subject_id text NOT NULL,
    month text NOT NULL,
    metric text NOT NULL,
    amount bigint NOT NULL CHECK (amount > 0),
    status text NOT NULL,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    request_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (month ~ '^[0-9]{4}-[0-9]{2}$')
);

CREATE TABLE IF NOT EXISTS mychat.usage_reservation_buckets (
    id text PRIMARY KEY,
    limit_subject_type text NOT NULL,
    limit_subject_id text NOT NULL,
    month text NOT NULL,
    metric text NOT NULL,
    reserved_amount bigint NOT NULL DEFAULT 0 CHECK (reserved_amount >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (month ~ '^[0-9]{4}-[0-9]{2}$')
);

CREATE TABLE IF NOT EXISTS mychat.dashboard_summaries (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    instagram_account_id text REFERENCES mychat.instagram_accounts(id) ON DELETE SET NULL,
    month text NOT NULL,
    expires_at timestamptz,
    generated_at timestamptz,
    summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (month ~ '^[0-9]{4}-[0-9]{2}$')
);

CREATE TABLE IF NOT EXISTS mychat.webhook_log (
    id text PRIMARY KEY,
    provider text,
    status text,
    received_at timestamptz,
    processed_at timestamptz,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.webhook_inbox (
    event_digest text PRIMARY KEY,
    status text NOT NULL DEFAULT 'pending',
    payload_encrypted text,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    owner text,
    fencing_token text,
    lease_until timestamptz,
    expires_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

CREATE TABLE IF NOT EXISTS mychat.webhook_processing_failures (
    id text PRIMARY KEY,
    event_id text,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    status text NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz,
    first_failed_at timestamptz,
    last_failed_at timestamptz,
    terminal_at timestamptz,
    error_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.instagram_automation_events (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    username_key text,
    stage text,
    created_at timestamptz NOT NULL,
    expires_at timestamptz,
    diagnostics jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.user_plans (
    id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES mychat.users(id) ON DELETE CASCADE,
    plan_key text NOT NULL,
    status text,
    starts_at timestamptz,
    ends_at timestamptz,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.user_limit_overrides (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    created_by_user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    status text,
    starts_at timestamptz,
    ends_at timestamptz,
    reason text,
    limits jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz,
    updated_at timestamptz,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.user_notification_preferences (
    id text PRIMARY KEY,
    user_id text NOT NULL REFERENCES mychat.users(id) ON DELETE CASCADE,
    email_enabled boolean,
    product_updates boolean,
    security_alerts boolean,
    preferences jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.admin_members (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    email text,
    role text,
    created_at timestamptz,
    disabled_at timestamptz,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.admin_audit_logs (
    id text PRIMARY KEY,
    admin_user_id text,
    target_user_id text,
    action text NOT NULL,
    created_at timestamptz NOT NULL,
    change_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.data_deletion_requests (
    id text PRIMARY KEY,
    user_id text,
    provider_subject_id text,
    status text,
    requested_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL,
    execution_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.oauth_code_consumed (
    id text PRIMARY KEY,
    provider text NOT NULL,
    code_hash text NOT NULL,
    created_at timestamptz NOT NULL,
    expires_at timestamptz,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.subscriptions (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    plan_key text NOT NULL,
    status text NOT NULL,
    provider text NOT NULL,
    provider_subscription_id text,
    provider_checkout_session_id text,
    current_period_start timestamptz,
    current_period_end timestamptz,
    cancelled_at timestamptz,
    cancellation_reason text,
    granted_by text,
    grant_reason text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS mychat.invoices (
    id text PRIMARY KEY,
    user_id text REFERENCES mychat.users(id) ON DELETE SET NULL,
    subscription_id text REFERENCES mychat.subscriptions(id) ON DELETE SET NULL,
    provider_event_id text,
    plan_key text,
    amount bigint CHECK (amount IS NULL OR amount >= 0),
    currency text,
    status text,
    provider text,
    description text,
    paid_at timestamptz,
    created_at timestamptz NOT NULL,
    receipt_url text,
    provider_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_extra jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS users_google_sub_uq
    ON mychat.users (google_sub) WHERE google_sub IS NOT NULL;
CREATE INDEX IF NOT EXISTS users_normalized_email_idx
    ON mychat.users (normalized_email) WHERE normalized_email IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS users_username_uq
    ON mychat.users (username) WHERE username IS NOT NULL;
CREATE INDEX IF NOT EXISTS users_email_verification_hash_idx
    ON mychat.users (email_verification_token_hash)
    WHERE email_verification_token_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS users_password_reset_hash_idx
    ON mychat.users (password_reset_token_hash)
    WHERE password_reset_token_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS users_status_idx ON mychat.users (status) WHERE status IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS instagram_accounts_owner_external_uq
    ON mychat.instagram_accounts (user_id, instagram_account_id)
    WHERE user_id IS NOT NULL AND instagram_account_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS instagram_accounts_active_external_uq
    ON mychat.instagram_accounts (instagram_account_id)
    WHERE instagram_account_id IS NOT NULL AND is_active IS TRUE AND connection_valid IS TRUE;
CREATE INDEX IF NOT EXISTS instagram_accounts_refresh_due_idx
    ON mychat.instagram_accounts (is_active, connection_valid, token_expires_at);
CREATE INDEX IF NOT EXISTS instagram_accounts_user_active_idx
    ON mychat.instagram_accounts (user_id, is_active);
CREATE INDEX IF NOT EXISTS instagram_accounts_ig_user_idx
    ON mychat.instagram_accounts (ig_user_id) WHERE ig_user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS instagram_trial_claim_uq
    ON mychat.instagram_account_trial_claims (instagram_account_id, plan_trial_identifier);

CREATE INDEX IF NOT EXISTS automations_owner_account_status_idx
    ON mychat.automations (user_id, instagram_account_id, status);
CREATE INDEX IF NOT EXISTS dm_rules_owner_active_idx ON mychat.dm_rules (user_id, is_active);
CREATE INDEX IF NOT EXISTS dm_rules_owner_account_active_idx
    ON mychat.dm_rules (user_id, instagram_account_id, is_active);
CREATE INDEX IF NOT EXISTS contacts_owner_account_created_idx
    ON mychat.contacts (user_id, instagram_account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS conversations_owner_account_created_idx
    ON mychat.conversations (user_id, instagram_account_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS conversation_messages_ordinal_uq
    ON mychat.conversation_messages (conversation_id, source_ordinal)
    WHERE conversation_id IS NOT NULL AND source_ordinal IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS comments_external_uq
    ON mychat.comments (user_id, instagram_account_id, ig_comment_id)
    WHERE user_id IS NOT NULL AND instagram_account_id IS NOT NULL AND ig_comment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS comments_owner_account_created_idx
    ON mychat.comments (user_id, instagram_account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS comments_opening_dedupe_idx
    ON mychat.comments (user_id, instagram_account_id, opening_dedupe_key, created_at DESC)
    WHERE opening_dedupe_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS comments_queue_idx
    ON mychat.comments (next_retry_at, id)
    WHERE action_status IN ('queued', 'retry', 'processing');

CREATE INDEX IF NOT EXISTS comment_sessions_owner_account_created_idx
    ON mychat.comment_dm_sessions (user_id, instagram_account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS comment_sessions_opening_dedupe_idx
    ON mychat.comment_dm_sessions (user_id, instagram_account_id, opening_dedupe_key, created_at DESC)
    WHERE opening_dedupe_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS comment_sessions_recipient_status_idx
    ON mychat.comment_dm_sessions (user_id, recipient_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS comment_sessions_expiry_idx ON mychat.comment_dm_sessions (expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS dm_logs_dedup_uq
    ON mychat.dm_logs (user_id, instagram_account_id, dedup_key)
    WHERE user_id IS NOT NULL AND instagram_account_id IS NOT NULL AND dedup_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS dm_logs_owner_account_created_idx
    ON mychat.dm_logs (user_id, instagram_account_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS instagram_media_external_uq
    ON mychat.instagram_media_catalog (instagram_account_id, media_id)
    WHERE instagram_account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS instagram_media_timestamp_idx
    ON mychat.instagram_media_catalog (instagram_account_id, media_timestamp DESC);
CREATE UNIQUE INDEX IF NOT EXISTS tracked_links_short_code_uq
    ON mychat.tracked_links (short_code) WHERE short_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS tracked_links_rule_idx
    ON mychat.tracked_links (user_id, instagram_account_id, rule_id);
CREATE INDEX IF NOT EXISTS tracked_links_created_idx
    ON mychat.tracked_links (user_id, instagram_account_id, created_at DESC);
CREATE INDEX IF NOT EXISTS tracked_links_expiry_idx ON mychat.tracked_links (expires_at);
CREATE INDEX IF NOT EXISTS link_click_link_idx ON mychat.link_click_events (tracked_link_id);
CREATE INDEX IF NOT EXISTS link_click_short_code_idx ON mychat.link_click_events (short_code);
CREATE INDEX IF NOT EXISTS link_click_owner_time_idx
    ON mychat.link_click_events (user_id, instagram_account_id, clicked_at DESC);
CREATE INDEX IF NOT EXISTS link_click_owner_ig_user_idx
    ON mychat.link_click_events (user_id, instagram_account_id, instagram_user_id);
CREATE INDEX IF NOT EXISTS link_click_expiry_idx ON mychat.link_click_events (expires_at);

CREATE INDEX IF NOT EXISTS usage_events_user_month_idx
    ON mychat.usage_events (user_id, event_month);
CREATE INDEX IF NOT EXISTS usage_events_user_type_month_idx
    ON mychat.usage_events (user_id, event_type, event_month);
CREATE INDEX IF NOT EXISTS usage_events_user_type_date_idx
    ON mychat.usage_events (user_id, event_type, event_date DESC);
CREATE INDEX IF NOT EXISTS usage_events_account_month_idx
    ON mychat.usage_events (instagram_account_id, event_month);
CREATE INDEX IF NOT EXISTS usage_events_subject_month_idx
    ON mychat.usage_events (event_month, limit_subject_type, limit_subject_id);
CREATE INDEX IF NOT EXISTS usage_events_automation_month_idx
    ON mychat.usage_events (automation_id, event_month);
CREATE INDEX IF NOT EXISTS usage_events_created_idx ON mychat.usage_events (created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS usage_reservations_idempotency_uq
    ON mychat.usage_reservations (idempotency_key);
CREATE INDEX IF NOT EXISTS usage_reservations_subject_status_idx
    ON mychat.usage_reservations (limit_subject_type, limit_subject_id, month, metric, status);
CREATE INDEX IF NOT EXISTS usage_reservations_expiry_status_idx
    ON mychat.usage_reservations (expires_at, status);
CREATE UNIQUE INDEX IF NOT EXISTS usage_reservation_buckets_subject_uq
    ON mychat.usage_reservation_buckets (limit_subject_type, limit_subject_id, month, metric);
CREATE INDEX IF NOT EXISTS monthly_usage_user_month_idx
    ON mychat.monthly_usage (user_id, event_month);
CREATE UNIQUE INDEX IF NOT EXISTS monthly_usage_subject_uq
    ON mychat.monthly_usage (event_month, limit_subject_type, limit_subject_id)
    WHERE limit_subject_type IS NOT NULL AND limit_subject_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS dashboard_summaries_owner_account_month_uq
    ON mychat.dashboard_summaries (user_id, instagram_account_id, month)
    WHERE user_id IS NOT NULL AND instagram_account_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS dashboard_summaries_user_month_idx
    ON mychat.dashboard_summaries (user_id, month);
CREATE INDEX IF NOT EXISTS dashboard_summaries_account_month_idx
    ON mychat.dashboard_summaries (instagram_account_id, month);
CREATE INDEX IF NOT EXISTS dashboard_summaries_expiry_idx
    ON mychat.dashboard_summaries (expires_at);

CREATE INDEX IF NOT EXISTS webhook_log_received_idx ON mychat.webhook_log (received_at DESC);
CREATE INDEX IF NOT EXISTS webhook_inbox_claim_idx
    ON mychat.webhook_inbox (available_at, event_digest)
    WHERE status IN ('pending', 'processing');
CREATE INDEX IF NOT EXISTS webhook_inbox_expiry_idx ON mychat.webhook_inbox (expires_at);
CREATE INDEX IF NOT EXISTS webhook_failures_retry_idx
    ON mychat.webhook_processing_failures (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS webhook_failures_first_failed_idx
    ON mychat.webhook_processing_failures (first_failed_at DESC);
CREATE INDEX IF NOT EXISTS webhook_failures_terminal_idx
    ON mychat.webhook_processing_failures (terminal_at) WHERE terminal_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS instagram_events_username_created_idx
    ON mychat.instagram_automation_events (username_key, created_at DESC);
CREATE INDEX IF NOT EXISTS instagram_events_stage_created_idx
    ON mychat.instagram_automation_events (stage, created_at DESC);
CREATE INDEX IF NOT EXISTS instagram_events_expiry_idx
    ON mychat.instagram_automation_events (expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS user_plans_user_uq ON mychat.user_plans (user_id);
CREATE INDEX IF NOT EXISTS user_limit_overrides_user_status_idx
    ON mychat.user_limit_overrides (user_id, status);
CREATE INDEX IF NOT EXISTS user_limit_overrides_window_idx
    ON mychat.user_limit_overrides (user_id, starts_at, ends_at);
CREATE INDEX IF NOT EXISTS user_limit_overrides_status_end_idx
    ON mychat.user_limit_overrides (status, ends_at);
CREATE UNIQUE INDEX IF NOT EXISTS user_notification_preferences_user_uq
    ON mychat.user_notification_preferences (user_id);
CREATE UNIQUE INDEX IF NOT EXISTS admin_members_user_uq
    ON mychat.admin_members (user_id) WHERE user_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS admin_members_email_uq
    ON mychat.admin_members (email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS admin_members_role_created_idx
    ON mychat.admin_members (role, created_at DESC);
CREATE INDEX IF NOT EXISTS admin_audit_admin_created_idx
    ON mychat.admin_audit_logs (admin_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS admin_audit_target_created_idx
    ON mychat.admin_audit_logs (target_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS admin_audit_action_created_idx
    ON mychat.admin_audit_logs (action, created_at DESC);
CREATE INDEX IF NOT EXISTS deletion_requests_created_idx
    ON mychat.data_deletion_requests (created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS oauth_code_provider_hash_uq
    ON mychat.oauth_code_consumed (provider, code_hash);
CREATE INDEX IF NOT EXISTS oauth_code_expiry_idx ON mychat.oauth_code_consumed (expires_at);

CREATE INDEX IF NOT EXISTS subscriptions_user_status_created_idx
    ON mychat.subscriptions (user_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS subscriptions_provider_subscription_idx
    ON mychat.subscriptions (provider_subscription_id)
    WHERE provider_subscription_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS subscriptions_provider_checkout_idx
    ON mychat.subscriptions (provider_checkout_session_id)
    WHERE provider_checkout_session_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS invoices_provider_event_uq
    ON mychat.invoices (provider_event_id) WHERE provider_event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS invoices_user_created_idx
    ON mychat.invoices (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS invoices_subscription_idx ON mychat.invoices (subscription_id);

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

COMMIT;