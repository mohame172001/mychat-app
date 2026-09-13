"""Prepare a clean Replit development schema, preserving the previous schema."""
from __future__ import annotations

import argparse
import hashlib
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql


def migration_body(text: str) -> str:
    lines = []
    for line in text.splitlines():
        if line in (r"\set ON_ERROR_STOP on", "BEGIN;", "COMMIT;"):
            continue
        if line.lstrip().startswith("\\"):
            raise ValueError("Unsupported psql directive in migration")
        lines.append(line)
    return "\n".join(lines)


def prepare() -> None:
    if not (os.getenv("REPL_ID") or os.getenv("REPL_SLUG")):
        raise RuntimeError("Run this command in the Replit development workspace")
    if os.getenv("REPLIT_DEPLOYMENT") == "1" or any(
        os.getenv(key, "").lower() in {"prod", "production"}
        for key in ("APP_ENV", "ENVIRONMENT", "ENV", "RAILWAY_ENVIRONMENT")
    ):
        raise RuntimeError("Fresh initialization is not allowed in production")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    migrations = []
    for path in sorted(Path(__file__).with_name("migrations").glob("*.sql")):
        if not re.fullmatch(r"\d{3}_[a-z0-9_]+\.sql", path.name):
            raise ValueError("Invalid migration filename")
        raw = path.read_bytes()
        migrations.append((path.stem.split("_", 1), hashlib.sha256(raw).hexdigest(),
                           migration_body(raw.decode("utf-8"))))
    if not migrations:
        raise RuntimeError("No migrations found")
    backup = None
    with psycopg.connect(database_url) as connection:
        with connection.transaction():
            connection.execute("SELECT pg_advisory_xact_lock(hashtext('mychat-schema-migrations'))")
            exists = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'mychat')"
            ).fetchone()[0]
            if exists:
                backup = "mychat_backup_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
                connection.execute(sql.SQL("ALTER SCHEMA mychat RENAME TO {}").format(sql.Identifier(backup)))
            for (version, description), checksum, body in migrations:
                connection.execute(body)
                connection.execute(
                    "INSERT INTO mychat.schema_migrations (version, description, checksum_sha256) "
                    "VALUES (%s, %s, %s)", (version, description, checksum)
                )
            counts = connection.execute(
                "SELECT (SELECT count(*) FROM mychat.users), "
                "(SELECT count(*) FROM mychat.instagram_accounts), "
                "(SELECT count(*) FROM mychat.automations)"
            ).fetchone()
            if counts != (0, 0, 0):
                raise RuntimeError("Fresh schema unexpectedly contains application records")
    print(f"Fresh schema ready: mychat; migrations={len(migrations)}")
    if backup:
        print(f"Previous development schema preserved: {backup}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fresh-start", action="store_true", required=True,
                        help="Start fresh and retain any existing schema as a backup")
    parser.parse_args()
    prepare()
