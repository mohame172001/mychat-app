# MyChaat domain cutover

Public origin: https://mychaat.net

Production Replit startup reads backend/public_site.json and sets FRONTEND_URL
and BACKEND_PUBLIC_URL to the canonical origin. Development URLs are unchanged.
Run python backend/scripts/rebrand_public_site.py before the frontend build.
The script changes public copy, not localStorage keys, token namespaces or IDs.

Before changing Meta delivery, verify production database users/accounts and
credential readiness. Do not delete the Railway callback while it serves existing
accounts. Adding a domain is not a migration of their data or encryption keys.

External configuration checklist (not completed by this document):
- Verify the domain-registration email.
- Meta app domain: mychaat.net; website: https://mychaat.net
- Instagram redirect: https://mychaat.net/api/instagram/callback
- Webhook: https://mychaat.net/api/instagram/webhook (verify signed delivery first)
- Privacy: https://mychaat.net/privacy
- Terms: https://mychaat.net/terms
- Deletion instructions: https://mychaat.net/data-deletion
- Google: add https://mychaat.net to authorized JavaScript origins. The current
  flow posts a Google Identity Services credential to /api/auth/google; it does
  not use a backend Google redirect callback.
- Payment provider website/return/webhook URLs: update only the active provider;
  no real payment integration is activated merely by this domain change.
- Do not invent support@mychaat.net: mailbox provisioning is separate.

Validate homepage, login, signup, legal pages, API health, signed webhook,
real account connection and existing data before retiring old domains.
