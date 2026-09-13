import unittest

from app.repositories.postgres_users import (
    PostgresUserRepository,
    PostgresUserRepositoryError,
    UnsafePostgresUsersEnvironment,
    development_postgres_users_repository,
)


class PostgresUserDocumentTests(unittest.TestCase):
    def test_import_preserves_nested_metadata_without_mutation(self):
        source = {
            "id": "imported-user",
            "name": "Current name",
            "profile": {"name": "Old name", "timezone": "Europe/Berlin"},
            "source_extra": {"legacy_flag": True},
            "session_version": 3,
        }
        columns, profile, extra = PostgresUserRepository._split_document(source)
        self.assertEqual(columns["id"], "imported-user")
        self.assertEqual(profile, {"name": "Current name", "timezone": "Europe/Berlin"})
        self.assertEqual(extra, {"legacy_flag": True, "session_version": 3})
        self.assertEqual(source["profile"]["name"], "Old name")

    def test_nested_import_does_not_bypass_token_protection(self):
        for field in ("profile", "source_extra"):
            with self.subTest(field=field):
                with self.assertRaises(PostgresUserRepositoryError):
                    PostgresUserRepository._split_document({field: {"nested": {"access_token": "secret"}}})

    def test_invalid_metadata_is_rejected_instead_of_lost(self):
        for value in (None, [], "metadata"):
            with self.subTest(value=value):
                with self.assertRaises(PostgresUserRepositoryError):
                    PostgresUserRepository._split_document({"profile": value})


class PostgresUsersRepositorySelectionTests(unittest.TestCase):
    def test_repository_is_unselected_by_default(self):
        self.assertIsNone(development_postgres_users_repository({}))

    def test_opt_in_rejects_production_or_non_replit_processes(self):
        unsafe_environments = (
            {
                "REPLIT_POSTGRES_USERS_ENABLED": "1",
                "REPL_ID": "test",
                "DATABASE_URL": "postgresql://unused",
                "APP_ENV": "production",
            },
            {
                "REPLIT_POSTGRES_USERS_ENABLED": "1",
                "DATABASE_URL": "postgresql://unused",
                "APP_ENV": "development",
            },
            {
                "REPLIT_POSTGRES_USERS_ENABLED": "1",
                "REPL_ID": "test",
                "DATABASE_URL": "postgresql://unused",
                "REPLIT_DEPLOYMENT": "1",
            },
        )
        for environment in unsafe_environments:
            with self.subTest(environment=environment):
                with self.assertRaises(UnsafePostgresUsersEnvironment):
                    development_postgres_users_repository(environment)

    def test_explicit_replit_development_opt_in_constructs_repository(self):
        repository = development_postgres_users_repository(
            {
                "REPLIT_POSTGRES_USERS_ENABLED": "1",
                "REPL_ID": "test",
                "DATABASE_URL": "postgresql://unused",
                "APP_ENV": "development",
            }
        )
        self.assertIsNotNone(repository)

    def test_explicit_empty_mapping_does_not_fall_back_to_process_environment(self):
        self.assertIsNone(development_postgres_users_repository({}))
