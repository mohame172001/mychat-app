\set ON_ERROR_STOP on

BEGIN;

-- Exact PostgreSQL counterpart of the Mongo bootstrap index declared at
-- backend/server.py:31135-31138. The optimized partial claim index remains as an
-- additional PostgreSQL-specific index.
CREATE INDEX IF NOT EXISTS comments_action_retry_lock_idx
    ON mychat.comments (action_status, next_retry_at, queue_lock_until);

COMMIT;