import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch
from cryptography.fernet import Fernet
from replit_start import configure, add_bundled_dependencies


class ReplitConfigurationTests(unittest.TestCase):
    def test_bundled_dependencies_added_once(self):
        with tempfile.TemporaryDirectory() as directory, patch('sys.path', ['existing']):
            root = Path(directory)
            (root / '.replit-deps').mkdir()
            add_bundled_dependencies(root)
            add_bundled_dependencies(root)
            import sys
            self.assertEqual(sys.path, [str(root / '.replit-deps'), 'existing'])

    def test_missing_bundle_preserves_development_imports(self):
        with tempfile.TemporaryDirectory() as directory, patch('sys.path', ['existing']):
            add_bundled_dependencies(Path(directory))
            import sys
            self.assertEqual(sys.path, ['existing'])

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

    def test_deployment_overrides_inherited_development_mode(self):
        env=self.environment()
        env.update(REPLIT_DEPLOYMENT="1",APP_ENV="development",
                   REPLIT_DOMAINS="published.example.com,alias.example.com")
        configure(env)
        self.assertEqual(env["APP_ENV"],"production")
        self.assertEqual(env["FRONTEND_URL"],"https://published.example.com")
        self.assertEqual(env["BACKEND_PUBLIC_URL"],"https://published.example.com")
        self.assertNotIn("IG_APP_SECRET",env)

    def test_deployment_does_not_fall_back_to_preview_domain(self):
        env=self.environment()
        env["REPLIT_DEPLOYMENT"]="1"
        with self.assertRaises(RuntimeError):
            configure(env)

    def test_explicit_production_origins_are_preserved(self):
        env=self.environment()
        env.update(REPLIT_DEPLOYMENT="1",FRONTEND_URL="https://app.example.com",
                   BACKEND_PUBLIC_URL="https://api.example.com")
        configure(env)
        self.assertEqual(env["APP_ENV"],"production")
        self.assertEqual(env["FRONTEND_URL"],"https://app.example.com")
        self.assertEqual(env["BACKEND_PUBLIC_URL"],"https://api.example.com")
