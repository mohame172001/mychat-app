import unittest
from public_site_security import default_content_security_policy


class PublicSiteSecurityTests(unittest.TestCase):
    def test_spa_can_load_own_assets(self):
        policy = default_content_security_policy('/login', 'text/html; charset=utf-8')
        self.assertIn("script-src 'self'", policy)
        self.assertIn("frame-ancestors 'none'", policy)
        self.assertIn("object-src 'none'", policy)
        self.assertNotIn('unsafe-eval', policy)

    def test_api_remains_locked_down(self):
        for path, content_type in [('/api/auth/login', 'application/json'),
                                   ('/api/instagram/callback', 'text/html'),
                                   ('/static/app.js', 'text/javascript')]:
            self.assertEqual(default_content_security_policy(path, content_type),
                             "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
