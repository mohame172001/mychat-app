"""Shared pytest configuration for the MyChat backend test suite.

Most pre-existing tests were written under the assumption that
`_handle_new_comment(source='polling')` would actually send a public
reply and opening DM when a matched rule fires. The production product
now defaults to polling-primary delivery, and the send gate remains
env-controlled so tests can still exercise diagnostics-only mode.

To keep legacy polling assertions passing without rewriting hundreds of
tests, we default the poller/send flags to "1" for every test process.
Tests that specifically exercise disabled or diagnostics-only polling
use `monkeypatch.delenv` / `monkeypatch.setenv` to assert the skip path.
"""
import os

# Module-load defaults (run once before any test imports server).
#
# The legacy polling tests expect the poller loop to be enabled and
# polling sends to be allowed. Keep that explicit in test setup so
# individual tests can monkeypatch the flags for disabled or
# diagnostics-only cases without depending on production defaults.
os.environ.setdefault('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED', '1')
os.environ.setdefault('IG_POLL_ENABLED', '1')

# Phase 2K: disable the auto-fire of the onboarding/parity helper from
# `_sync_user_instagram_account_doc` / `instagram_account_activate`
# during tests. The legacy token-refresh / multi-account tests mock
# `httpx.AsyncClient` with a fixed FIFO list of responses; the helper's
# background Graph subscribe would consume one of those responses and
# shift every other assertion downstream. Tests that exercise the
# helper directly (test_phase2k_account_onboarding_parity.py) bypass
# the flag because they call the helper themselves.
os.environ.setdefault('IG_AUTO_ENSURE_WEBHOOK_READY', '0')

# Phase 2M: legacy automation tests build fake accounts that never
# pass through `certify_instagram_account_for_comment_webhooks`, so
# their accounts lack the `commentWebhookReady=True` flag the gate
# now enforces. Set the flag OFF for tests so those existing
# scenarios continue to assert their original errors. The Phase 2M
# tests explicitly monkeypatch the flag back ON inside the test body.
os.environ.setdefault('IG_REQUIRE_COMMENT_WEBHOOK_CERT', '0')
