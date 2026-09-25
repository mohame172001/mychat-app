"""Separate the SPA document policy from the strict API response policy."""


def default_content_security_policy(path, content_type):
    if content_type.lower().startswith('text/html') and not path.startswith('/api/'):
        return (
            "default-src 'self'; script-src 'self' https://accounts.google.com/gsi/client; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://accounts.google.com; "
            "font-src 'self' https://fonts.gstatic.com; img-src 'self' data: blob: https:; "
            "connect-src 'self' https://accounts.google.com; frame-src https://accounts.google.com; "
            "object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'none'"
        )
    return "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
