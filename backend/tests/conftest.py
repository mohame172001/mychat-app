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
os.environ.setdefault('IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED', '1')
