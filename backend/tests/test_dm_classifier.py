"""Phase 2.18Z — unit tests for the extracted dm_classifier module.

Pure-function tests, no Mongo / FastAPI / httpx involved. These lock
down the contract so future tweaks to classify_instagram_send_error
keep working with the rest of the system (comment processing,
dashboard summary, admin failures view, Sentry classification).
"""
from __future__ import annotations

import json

import pytest

from dm_classifier import (
    _redact_secrets,
    _safe_provider_error_payload,
    classify_instagram_send_error,
    _detailed_send_result,
    PERMANENT_GRAPH_FAILURE_REASONS,
    TRANSIENT_GRAPH_FAILURE_REASONS,
)


# ---------- _redact_secrets --------------------------------------------------


def test_redact_secrets_replaces_known_keys():
    src = {
        'access_token': 'xyz',
        'meta_access_token': 'mt',
        'authorization': 'Bearer abc',
        'name': 'alice',
        'nested': {'refresh_token': 'r1', 'safe_field': 'ok'},
    }
    out = _redact_secrets(src)
    assert out['access_token'] == '***REDACTED***'
    assert out['meta_access_token'] == '***REDACTED***'
    assert out['authorization'] == '***REDACTED***'
    assert out['name'] == 'alice'
    assert out['nested']['refresh_token'] == '***REDACTED***'
    assert out['nested']['safe_field'] == 'ok'


def test_redact_secrets_is_case_insensitive():
    out = _redact_secrets({'ACCESS_TOKEN': 'xyz', 'Token': 'abc'})
    assert out['ACCESS_TOKEN'] == '***REDACTED***'
    assert out['Token'] == '***REDACTED***'


def test_redact_secrets_recurses_into_lists():
    out = _redact_secrets([{'access_token': 'x'}, {'name': 'a'}])
    assert out[0]['access_token'] == '***REDACTED***'
    assert out[1]['name'] == 'a'


def test_redact_secrets_pass_through_non_containers():
    assert _redact_secrets('hello') == 'hello'
    assert _redact_secrets(42) == 42
    assert _redact_secrets(None) is None


# ---------- _safe_provider_error_payload -------------------------------------


def test_safe_payload_unwraps_error_envelope():
    raw = json.dumps({'error': {'code': 1, 'message': 'oops', 'access_token': 'xyz'}})
    out = _safe_provider_error_payload(raw)
    # 'error' wrapper unwrapped → top-level code + message present
    assert out['code'] == 1
    assert out['message'] == 'oops'
    # secret scrubbed
    assert out['access_token'] == '***REDACTED***'


def test_safe_payload_handles_plain_strings():
    out = _safe_provider_error_payload('not json at all')
    assert isinstance(out, dict)
    assert 'message' in out


def test_safe_payload_handles_dict_input():
    out = _safe_provider_error_payload({'code': 17, 'message': 'rate limit'})
    assert out['code'] == 17
    assert out['message'] == 'rate limit'


# ---------- classify_instagram_send_error ------------------------------------


def test_classify_messaging_window_expired_subcode():
    """Meta returns code=1 + subcode 1545033 for 'recipient not eligible'."""
    err = {'code': 1, 'error_subcode': 1545033, 'message': 'An unknown error has occurred.'}
    out = classify_instagram_send_error(err, status_code=400)
    assert out['failure_reason'] == 'messaging_window_expired'
    assert out['retryable'] is False
    assert out['provider_code'] == 1
    assert out['provider_subcode'] == 1545033


@pytest.mark.parametrize('subcode', [1545033, 1545041, 2018028, 2018278])
def test_classify_all_messaging_window_subcodes(subcode):
    out = classify_instagram_send_error({'code': 1, 'error_subcode': subcode}, status_code=400)
    assert out['failure_reason'] == 'messaging_window_expired'
    assert out['retryable'] is False


def test_classify_rate_limit_by_status_429():
    out = classify_instagram_send_error({'message': 'whatever'}, status_code=429)
    assert out['failure_reason'] == 'rate_limited'
    assert out['retryable'] is True


def test_classify_rate_limit_by_meta_code():
    for code in (4, 17, 32, 613):
        out = classify_instagram_send_error({'code': code, 'message': 'x'}, status_code=400)
        assert out['failure_reason'] == 'rate_limited'
        assert out['retryable'] is True


def test_classify_server_error_5xx_is_temporary():
    for status in (500, 502, 503, 504):
        out = classify_instagram_send_error({'message': 'oops'}, status_code=status)
        assert out['failure_reason'] == 'temporary_graph_error'
        assert out['retryable'] is True


def test_classify_permission_error_codes():
    for code in (10, 190, 200):
        out = classify_instagram_send_error({'code': code, 'message': 'perm'}, status_code=400)
        assert out['failure_reason'] == 'permission_error'
        assert out['retryable'] is False


def test_classify_recipient_unavailable_phrases():
    for phrase in (
        'recipient is unavailable',
        'cannot receive',
        'no message thread',
        'outside the messaging window',
    ):
        out = classify_instagram_send_error({'message': phrase}, status_code=400)
        assert out['failure_reason'] == 'recipient_unavailable'
        assert out['retryable'] is False


def test_classify_blocked_messages():
    out = classify_instagram_send_error({'message': 'User has blocked you'}, status_code=400)
    assert out['failure_reason'] == 'user_blocked_messages'
    assert out['retryable'] is False


def test_classify_unknown_pattern_falls_back():
    out = classify_instagram_send_error({'message': 'something completely new'}, status_code=400)
    assert out['failure_reason'] == 'unknown_graph_error'
    assert out['retryable'] is False


def test_classify_permanent_reasons_force_retryable_false():
    """Even if a transient-looking timeout phrase coexists with a permanent
    code, the permanent classification wins and retryable stays False."""
    out = classify_instagram_send_error(
        {'code': 10, 'message': 'timeout happened'}, status_code=400,
    )
    assert out['failure_reason'] == 'permission_error'
    assert out['retryable'] is False


def test_classify_returns_stable_dict_shape():
    out = classify_instagram_send_error({}, status_code=400)
    assert set(out.keys()) >= {
        'failure_reason', 'retryable', 'provider_code',
        'provider_subcode', 'status_code', 'safe_label',
    }


# ---------- _detailed_send_result --------------------------------------------


def test_detailed_send_result_success():
    out = _detailed_send_result(True, status_code=200, body={'id': 'abc'})
    assert out['ok'] is True
    assert out['status'] == 'success'
    assert out['status_code'] == 200
    assert out['failure_reason'] is None
    assert out['retryable'] is False


def test_detailed_send_result_success_scrubs_body():
    out = _detailed_send_result(True, status_code=200, body={'access_token': 'leak', 'id': 'a'})
    assert out['body']['access_token'] == '***REDACTED***'
    assert out['body']['id'] == 'a'


def test_detailed_send_result_failure_classified():
    out = _detailed_send_result(
        False, status_code=400,
        error={'code': 1, 'error_subcode': 1545033},
    )
    assert out['ok'] is False
    assert out['status'] == 'failed'
    assert out['failure_reason'] == 'messaging_window_expired'
    assert out['retryable'] is False


# ---------- frozenset hygiene ------------------------------------------------


def test_failure_reason_sets_are_disjoint():
    assert PERMANENT_GRAPH_FAILURE_REASONS.isdisjoint(TRANSIENT_GRAPH_FAILURE_REASONS)


def test_failure_reason_sets_are_frozenset():
    assert isinstance(PERMANENT_GRAPH_FAILURE_REASONS, frozenset)
    assert isinstance(TRANSIENT_GRAPH_FAILURE_REASONS, frozenset)
