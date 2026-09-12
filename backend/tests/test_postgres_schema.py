"""Integration tests for the Phase 2A PostgreSQL schema.

These tests use only the Replit development DATABASE_URL and never import the
FastAPI application. They intentionally exercise the committed SQL directly.
"""
from __future__ import annotations

import os
import pathlib
import subprocess
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor


ROOT = pathlib.Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "backend/db/migrations/001_replit_postgres_foundation.sql"
MIGRATION_DIR = ROOT / "backend/db/migrations"
MIGRATION_RUNNER = ROOT / "backend/db/apply_migrations.sh"
MIGRATION_VERSIONS = ("001", "002", "003", "004")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
RUN_DB_TESTS = os.environ.get("RUN_REPLIT_DB_TESTS") == "1"
TEST_SCHEMA_ACK = os.environ.get("REPLIT_DB_TEST_SCHEMA_ACK", "")

SOURCE_COLLECTION_TABLES = {
    "admin_audit_logs",
    "admin_members",
    "automations",
    "broadcasts",
    "comment_dm_sessions",
    "comments",
    "contacts",
    "conversations",
    "dashboard_summaries",
    "data_deletion_requests",
    "dm_logs",
    "dm_rules",
    "instagram_account_trial_claims",
    "instagram_accounts",
    "instagram_automation_events",
    "instagram_media_catalog",
    "invoices",
    "link_click_events",
    "monthly_usage",
    "oauth_code_consumed",
    "subscriptions",
    "tracked_links",
    "usage_events",
    "usage_reservation_buckets",
    "usage_reservations",
    "user_limit_overrides",
    "user_notification_preferences",
    "user_plans",
    "users",
    "webhook_inbox",
    "webhook_log",
    "webhook_processing_failures",
}


def psql(sql: str, *, tuples: bool = True) -> str:
    if not DATABASE_URL:
        raise unittest.SkipTest("DATABASE_URL is not available")
    command = ["psql", DATABASE_URL, "-X", "-v", "ON_ERROR_STOP=1"]
    if tuples:
        command.extend(["-A", "-t"])
    command.extend(["-c", sql])
    completed = subprocess.run(command, check=True, text=True, capture_output=True)
    return completed.stdout.strip()


class PostgresSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if not DATABASE_URL:
            raise unittest.SkipTest("DATABASE_URL is not available")
        if not RUN_DB_TESTS:
            raise unittest.SkipTest("set RUN_REPLIT_DB_TESTS=1 for destructive development DB tests")
        if TEST_SCHEMA_ACK != "mychat":
            raise unittest.SkipTest("set REPLIT_DB_TEST_SCHEMA_ACK=mychat to confirm test cleanup scope")
        if not (os.environ.get("REPL_ID") or os.environ.get("REPL_SLUG")):
            raise unittest.SkipTest("tests require a Replit workspace")
        production_markers = (
            os.environ.get("APP_ENV", ""),
            os.environ.get("ENVIRONMENT", ""),
            os.environ.get("ENV", ""),
            os.environ.get("RAILWAY_ENVIRONMENT", ""),
        )
        if os.environ.get("REPLIT_DEPLOYMENT") == "1" or any(
            marker.lower() in {"production", "prod"} for marker in production_markers
        ):
            raise unittest.SkipTest("tests refuse deployment databases")
        subprocess.run(
            [str(MIGRATION_RUNNER)],
            check=True,
            text=True,
            capture_output=True,
            env={
                **os.environ,
                "DATABASE_URL": DATABASE_URL,
                "ALLOW_REPLIT_DEV_MIGRATIONS": "1",
            },
        )

    def tearDown(self) -> None:
        psql(
            "TRUNCATE mychat.webhook_inbox, mychat.usage_reservations "
            "RESTART IDENTITY CASCADE"
        )

    def test_all_source_collections_have_tables(self) -> None:
        rows = psql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='mychat' AND table_type='BASE TABLE'"
        ).splitlines()
        actual = set(rows)
        self.assertEqual(set(), SOURCE_COLLECTION_TABLES - actual)
        self.assertIn("conversation_messages", actual)
        self.assertIn("schema_migrations", actual)

    def test_typed_and_jsonb_columns_exist(self) -> None:
        rows = psql(
            "SELECT table_name || '.' || column_name || ':' || data_type "
            "FROM information_schema.columns WHERE table_schema='mychat'"
        ).splitlines()
        columns = set(rows)
        expected = {
            "users.id:text",
            "instagram_accounts.connection_valid:boolean",
            "comments.retry_count:integer",
            "comments.queue_lock_until:timestamp with time zone",
            "usage_reservations.amount:bigint",
            "usage_reservations.reservation_id:text",
            "usage_reservations.event_type:text",
            "usage_reservation_buckets.confirmed_amount:bigint",
            "webhook_inbox.payload_encrypted:text",
            "automations.nodes:jsonb",
            "automations.source_extra:jsonb",
        }
        self.assertEqual(set(), expected - columns)

    def test_unique_and_partial_indexes(self) -> None:
        rows = psql(
            "SELECT indexname || '|' || indexdef FROM pg_indexes "
            "WHERE schemaname='mychat'"
        ).splitlines()
        definitions = "\n".join(rows)
        for index in (
            "comments_external_uq",
            "dm_logs_dedup_uq",
            "instagram_accounts_active_external_uq",
            "usage_reservations_idempotency_uq",
            "monthly_usage_subject_uq",
            "oauth_code_provider_hash_uq",
            "instagram_trial_claim_claimant_created_idx",
            "instagram_accounts_external_idx",
            "instagram_events_created_idx",
            "user_limit_overrides_creator_created_idx",
            "admin_members_disabled_idx",
            "comments_action_retry_lock_idx",
        ):
            self.assertIn(index, definitions)
        self.assertIn("UNIQUE INDEX", definitions)
        self.assertIn(" WHERE ", definitions)
        self.assertIn("is_active IS TRUE", definitions)
        queue_index = psql(
            "SELECT string_agg(a.attname, ',' ORDER BY k.ordinality) "
            "FROM pg_class i "
            "JOIN pg_namespace n ON n.oid=i.relnamespace "
            "JOIN pg_index x ON x.indexrelid=i.oid "
            "JOIN LATERAL unnest(x.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true "
            "JOIN pg_class t ON t.oid=x.indrelid "
            "JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum "
            "WHERE n.nspname='mychat' AND i.relname='comments_action_retry_lock_idx'"
        )
        self.assertEqual(
            "action_status,next_retry_at,queue_lock_until",
            queue_index,
        )

    def test_usage_reservation_idempotency_is_unique(self) -> None:
        psql(
            "INSERT INTO mychat.usage_reservations "
            "(id,idempotency_key,limit_subject_type,limit_subject_id,month,metric,amount,status) "
            "VALUES ('r1','same-key','user','u1','2026-09','dm',1,'pending')"
        )
        duplicate = subprocess.run(
            [
                "psql",
                DATABASE_URL,
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "INSERT INTO mychat.usage_reservations "
                "(id,idempotency_key,limit_subject_type,limit_subject_id,month,metric,amount,status) "
                "VALUES ('r2','same-key','user','u1','2026-09','dm',1,'pending')",
            ],
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(0, duplicate.returncode)
        self.assertIn("usage_reservations_idempotency_uq", duplicate.stderr)

    def test_raw_instagram_token_import_is_blocked(self) -> None:
        user_id = f"token-user-{uuid.uuid4()}"
        account_id = f"token-account-{uuid.uuid4()}"
        try:
            psql(f"INSERT INTO mychat.users (id) VALUES ('{user_id}')")
            raw_user_token = subprocess.run(
                [
                    "psql",
                    DATABASE_URL,
                    "-X",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "UPDATE mychat.users SET meta_access_token='synthetic-raw-token' "
                    f"WHERE id='{user_id}'",
                ],
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(0, raw_user_token.returncode)
            self.assertIn("users_raw_meta_token_import_blocked", raw_user_token.stderr)

            psql(
                "INSERT INTO mychat.instagram_accounts (id,user_id) "
                f"VALUES ('{account_id}','{user_id}')"
            )
            raw_account_token = subprocess.run(
                [
                    "psql",
                    DATABASE_URL,
                    "-X",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "UPDATE mychat.instagram_accounts "
                    "SET access_token='synthetic-raw-token' "
                    f"WHERE id='{account_id}'",
                ],
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(0, raw_account_token.returncode)
            self.assertIn(
                "instagram_accounts_raw_token_import_blocked",
                raw_account_token.stderr,
            )
        finally:
            psql(
                "DELETE FROM mychat.instagram_accounts "
                f"WHERE id='{account_id}'; DELETE FROM mychat.users WHERE id='{user_id}'"
            )

    def test_webhook_claim_uses_skip_locked_and_fences_workers(self) -> None:
        sql = "\n".join(path.read_text() for path in sorted(MIGRATION_DIR.glob("*.sql")))
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        psql(
            "INSERT INTO mychat.webhook_inbox "
            "(event_digest,status,payload_encrypted,available_at) "
            "VALUES ('event-1','pending','ciphertext',clock_timestamp())"
        )

        def claim(worker: str) -> str:
            token = str(uuid.uuid4())
            return psql(
                "SELECT event_digest || '|' || owner || '|' || fencing_token "
                f"FROM mychat.claim_webhook_job('{worker}','{token}',interval '5 minutes')"
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(claim, ("worker-a", "worker-b")))
        claimed = [result for result in results if result]
        self.assertEqual(1, len(claimed))
        event_id, owner, fencing_token = claimed[0].split("|")
        self.assertEqual("event-1", event_id)
        self.assertIn(owner, {"worker-a", "worker-b"})
        self.assertTrue(fencing_token)
        self.assertEqual(
            "f",
            psql("SELECT mychat.complete_webhook_job('event-1','stale-token')"),
        )
        self.assertEqual(
            "t",
            psql(
                "SELECT mychat.complete_webhook_job"
                f"('event-1','{fencing_token}')"
            ),
        )
        self.assertEqual(
            "completed|true|true",
            psql(
                "SELECT status || '|' || (payload_encrypted IS NULL)::text || '|' || "
                "(fencing_token IS NULL)::text FROM mychat.webhook_inbox "
                "WHERE event_digest='event-1'"
            ),
        )

    def test_webhook_failure_retries_then_terminalizes(self) -> None:
        psql(
            "INSERT INTO mychat.webhook_inbox "
            "(event_digest,status,payload_encrypted,available_at,attempts) "
            "VALUES ('event-retry','pending','ciphertext',clock_timestamp(),3)"
        )
        claim = psql(
            "SELECT fencing_token FROM mychat.claim_webhook_job"
            "('worker-retry','token-retry',interval '5 minutes')"
        )
        self.assertEqual("token-retry", claim)
        self.assertEqual(
            "t",
            psql(
                "SELECT mychat.fail_webhook_job"
                "('event-retry','token-retry','temporary',interval '0 seconds')"
            ),
        )
        self.assertEqual(
            "pending|4|false",
            psql(
                "SELECT status || '|' || attempts || '|' || "
                "(payload_encrypted IS NULL)::text FROM mychat.webhook_inbox "
                "WHERE event_digest='event-retry'"
            ),
        )
        psql(
            "SELECT event_digest FROM mychat.claim_webhook_job"
            "('worker-final','token-final',interval '5 minutes')"
        )
        self.assertEqual(
            "t",
            psql(
                "SELECT mychat.fail_webhook_job"
                "('event-retry','token-final','terminal',interval '0 seconds')"
            ),
        )
        self.assertEqual(
            "failed|5|true",
            psql(
                "SELECT status || '|' || attempts || '|' || "
                "(payload_encrypted IS NULL)::text FROM mychat.webhook_inbox "
                "WHERE event_digest='event-retry'"
            ),
        )

    def test_expired_fifth_attempt_is_terminalized(self) -> None:
        psql(
            "INSERT INTO mychat.webhook_inbox "
            "(event_digest,status,payload_encrypted,attempts,available_at,lease_until,"
            "owner,fencing_token) VALUES "
            "('event-crashed','processing','ciphertext',5,clock_timestamp()-interval '1 minute',"
            "clock_timestamp()-interval '1 second','dead-worker','dead-token')"
        )
        self.assertEqual(
            "",
            psql(
                "SELECT event_digest FROM mychat.claim_webhook_job"
                "('new-worker','new-token',interval '5 minutes')"
            ),
        )
        self.assertEqual(
            "failed|true|true",
            psql(
                "SELECT status || '|' || (payload_encrypted IS NULL)::text || '|' || "
                "(fencing_token IS NULL)::text FROM mychat.webhook_inbox "
                "WHERE event_digest='event-crashed'"
            ),
        )

    def test_runner_rejects_production_markers_and_missing_opt_in(self) -> None:
        base_env = {**os.environ, "DATABASE_URL": DATABASE_URL}
        without_opt_in = subprocess.run(
            [str(MIGRATION_RUNNER)], text=True, capture_output=True, env=base_env
        )
        self.assertNotEqual(0, without_opt_in.returncode)
        self.assertIn("ALLOW_REPLIT_DEV_MIGRATIONS", without_opt_in.stderr)

        for key, value in (
            ("APP_ENV", "production"),
            ("ENVIRONMENT", "prod"),
            ("ENV", "PRODUCTION"),
            ("RAILWAY_ENVIRONMENT", "production"),
            ("REPLIT_DEPLOYMENT", "1"),
        ):
            env = {
                **base_env,
                "ALLOW_REPLIT_DEV_MIGRATIONS": "1",
                key: value,
            }
            rejected = subprocess.run(
                [str(MIGRATION_RUNNER)], text=True, capture_output=True, env=env
            )
            self.assertNotEqual(0, rejected.returncode, key)
            self.assertIn("Refusing", rejected.stderr, key)

    def test_runner_rejects_checksum_drift(self) -> None:
        original = psql(
            "SELECT checksum_sha256 FROM mychat.schema_migrations WHERE version='001'"
        )
        try:
            psql(
                "UPDATE mychat.schema_migrations SET checksum_sha256='wrong' "
                "WHERE version='001'"
            )
            result = subprocess.run(
                [str(MIGRATION_RUNNER)],
                text=True,
                capture_output=True,
                env={
                    **os.environ,
                    "DATABASE_URL": DATABASE_URL,
                    "ALLOW_REPLIT_DEV_MIGRATIONS": "1",
                },
            )
            self.assertNotEqual(0, result.returncode)
            self.assertIn("checksum mismatch", result.stderr)
        finally:
            psql(
                "UPDATE mychat.schema_migrations "
                f"SET checksum_sha256='{original}' WHERE version='001'"
            )

    def test_runner_rejects_null_legacy_checksum(self) -> None:
        original = psql(
            "SELECT checksum_sha256 FROM mychat.schema_migrations WHERE version='001'"
        )
        try:
            psql(
                "UPDATE mychat.schema_migrations SET checksum_sha256=NULL "
                "WHERE version='001'"
            )
            result = subprocess.run(
                [str(MIGRATION_RUNNER)],
                text=True,
                capture_output=True,
                env={
                    **os.environ,
                    "DATABASE_URL": DATABASE_URL,
                    "ALLOW_REPLIT_DEV_MIGRATIONS": "1",
                },
            )
            self.assertNotEqual(0, result.returncode)
            self.assertIn("unsafe legacy adoption", result.stderr)
        finally:
            psql(
                "UPDATE mychat.schema_migrations "
                f"SET checksum_sha256='{original}' WHERE version='001'"
            )

    def test_concurrent_runners_are_idempotent(self) -> None:
        env = {
            **os.environ,
            "DATABASE_URL": DATABASE_URL,
            "ALLOW_REPLIT_DEV_MIGRATIONS": "1",
        }

        def run_migrations(_: int) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [str(MIGRATION_RUNNER)], text=True, capture_output=True, env=env
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(run_migrations, range(2)))
        for result in results:
            self.assertEqual(0, result.returncode, result.stderr)

    def test_migration_is_idempotent(self) -> None:
        before = psql(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='mychat'"
        )
        subprocess.run(
            [str(MIGRATION_RUNNER)],
            check=True,
            text=True,
            capture_output=True,
            env={
                **os.environ,
                "DATABASE_URL": DATABASE_URL,
                "ALLOW_REPLIT_DEV_MIGRATIONS": "1",
            },
        )
        after = psql(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema='mychat'"
        )
        self.assertEqual(before, after)
        version_list = ",".join(f"'{version}'" for version in MIGRATION_VERSIONS)
        self.assertEqual(
            str(len(MIGRATION_VERSIONS)),
            psql(
                "SELECT count(*) FROM mychat.schema_migrations "
                f"WHERE version IN ({version_list}) AND checksum_sha256 IS NOT NULL"
            ),
        )


if __name__ == "__main__":
    unittest.main()