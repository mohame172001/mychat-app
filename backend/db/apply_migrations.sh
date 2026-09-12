#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required" >&2
  exit 1
fi

if [[ "${ALLOW_REPLIT_DEV_MIGRATIONS:-}" != "1" ]]; then
  echo "Set ALLOW_REPLIT_DEV_MIGRATIONS=1 to apply development migrations" >&2
  exit 1
fi

for marker in \
  "${APP_ENV:-}" \
  "${ENVIRONMENT:-}" \
  "${ENV:-}" \
  "${RAILWAY_ENVIRONMENT:-}"
do
  normalized="$(printf '%s' "$marker" | tr '[:upper:]' '[:lower:]')"
  if [[ "$normalized" == "production" || "$normalized" == "prod" ]]; then
    echo "Refusing to apply development migrations in a production environment" >&2
    exit 1
  fi
done
if [[ "${REPLIT_DEPLOYMENT:-}" == "1" ]]; then
  echo "Refusing to apply development migrations from a deployment" >&2
  exit 1
fi

if [[ -z "${REPL_ID:-}" && -z "${REPL_SLUG:-}" ]]; then
  echo "Refusing to apply migrations outside a Replit workspace" >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Bootstrap only the migration ledger so an older Phase 2A draft can be adopted
# safely. Application tables remain exclusively owned by versioned migrations.
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE SCHEMA IF NOT EXISTS mychat;
CREATE TABLE IF NOT EXISTS mychat.schema_migrations (
  version text PRIMARY KEY,
  description text NOT NULL,
  checksum_sha256 text,
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE mychat.schema_migrations
  ADD COLUMN IF NOT EXISTS checksum_sha256 text;
SQL

for migration in "$root"/backend/db/migrations/*.sql; do
  filename="$(basename "$migration")"
  version="${filename%%_*}"
  description="${filename#*_}"
  description="${description%.sql}"
  if [[ ! "$filename" =~ ^[0-9]{3}_[a-z0-9_]+\.sql$ ]]; then
    echo "Invalid migration filename: $filename" >&2
    exit 1
  fi
  checksum="$(sha256sum "$migration" | awk '{print $1}')"

  body="$(sed '/^\\set ON_ERROR_STOP on$/d; /^BEGIN;$/d; /^COMMIT;$/d' "$migration")"
  temp_sql="$(mktemp)"
  trap 'rm -f "$temp_sql"' EXIT
  {
    echo '\set ON_ERROR_STOP on'
    echo 'BEGIN;'
    echo "SELECT pg_advisory_xact_lock(hashtext('mychat-schema-migrations'));"
    cat <<SQL
DO \$migration_guard\$
DECLARE
  recorded_checksum text;
BEGIN
  SELECT checksum_sha256
  INTO recorded_checksum
  FROM mychat.schema_migrations
  WHERE version = '$version';

  IF FOUND AND recorded_checksum IS NULL THEN
    RAISE EXCEPTION 'migration $version has no checksum; refusing unsafe legacy adoption';
  ELSIF FOUND AND recorded_checksum <> '$checksum' THEN
    RAISE EXCEPTION 'migration $version checksum mismatch; historical migrations are immutable';
  END IF;
END;
\$migration_guard\$;
SELECT NOT EXISTS (
  SELECT 1 FROM mychat.schema_migrations WHERE version = '$version'
) AS apply_migration \gset
\if :apply_migration
\echo Applying $filename to the Replit development database
SQL
    printf '%s\n' "$body"
    printf "INSERT INTO mychat.schema_migrations (version, description, checksum_sha256) VALUES ('%s', '%s', '%s');\n" \
      "$version" "$description" "$checksum"
    cat <<SQL
\else
\echo Skipping already applied $filename
\endif
SQL
    echo 'COMMIT;'
  } > "$temp_sql"
  psql "$DATABASE_URL" -X -f "$temp_sql"
  rm -f "$temp_sql"
  trap - EXIT
done