import unittest
from cryptography.fernet import Fernet
from replit_start import configure


class ReplitConfigurationTests(unittest.TestCase):
    def environment(self):
        return {"DATABASE_URL":"synthetic-dsn","SESSION_SECRET":"s"*48,"REPLIT_DEV_DOMAIN":"preview.example.com"}

    def test_stable_distinct_server_keys(self):
        one,two=self.environment(),self.environment()
        configure(one)
        configure(two)
        for key in ("JWT_SECRET","INSTAGRAM_TOKEN_ENCRYPTION_KEY","WEBHOOK_INBOX_ENCRYPTION_KEY"):
            self.assertEqual(one[key],two[key])
        self.assertEqual(len({one[key] for key in ("JWT_SECRET","INSTAGRAM_TOKEN_ENCRYPTION_KEY","WEBHOOK_INBOX_ENCRYPTION_KEY")}),3)
        Fernet(one["INSTAGRAM_TOKEN_ENCRYPTION_KEY"].encode())
        self.assertEqual(one["DB_BACKEND"],"postgres")
        self.assertEqual(one["BACKEND_PUBLIC_URL"],"https://preview.example.com")
        self.assertNotIn("META_APP_SECRET",one)

    def test_explicit_secrets_preserved(self):
        env=self.environment()
        env["JWT_SECRET"]="existing-key"
        configure(env)
        self.assertEqual(env["JWT_SECRET"],"existing-key")

    def test_missing_master_fails(self):
        env=self.environment()
        del env["SESSION_SECRET"]
        with self.assertRaises(RuntimeError):
            configure(env)
