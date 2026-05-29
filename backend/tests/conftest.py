"""Shared pytest configuration for the MyChat backend test suite.

Most pre-existing tests were written under the assumption that
`_handle_new_comment(source='polling')` would actually send a public
reply and opening DM when a matched rule fires. Phase 2G flipped the
default: polling no longer sends by default — the SaaS product treats
webhook as the primary path and the env flag
`IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED=1` opts polling back
into legacy "send on discovery" behavior.

To keep the legacy assertions passing without rewriting hundreds of
tests, we default the env flag to "1" for every test process. New
tests that specifically exercise the Phase 2G gate use
`monkeypatch.delenv` / `monkeypatch.setenv` to restore the production
default of OFF and assert the skip path.
"""
import os

# Module-load defaults (run once before any test imports server).
#
# Phase 2H: the production default for IG_POLL_ENABLED is now '0' so the
# poller loop does NOT run on a brand-new deployment — webhook is the
# sole comment-automation send path. The legacy test suite, however,
# was written when IG_POLL_ENABLED defaulted to '1' AND polling was the
# primary sender. We restore that environment here so the 30+ polling
# pipeline tests keep their existing assertions valid without rewrites.
#
# Tests that specifically exercise the Phase 2H default-OFF posture or
# the Phase 2G send gate use `monkeypatch.delenv` / `monkeypatch.setenv`
# to remove or flip these flags inside the test body.
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
