import unittest

from app.repositories.postgres_users import (
    UnsafePostgresUsersEnvironment,
    development_postgres_users_repository,
)


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