"""Phase 2.18Z — unit tests for password_policy module."""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from password_policy import (
    PASSWORD_MIN_LENGTH,
    PASSWORD_MAX_LENGTH,
    enforce_password_policy,
)


def test_min_max_constants_sane():
    assert PASSWORD_MIN_LENGTH == 8
    assert PASSWORD_MAX_LENGTH == 256
    assert PASSWORD_MIN_LENGTH < PASSWORD_MAX_LENGTH


def test_accepts_min_length_password():
    pw = 'x' * PASSWORD_MIN_LENGTH
    assert enforce_password_policy(pw) == pw


def test_accepts_max_length_password():
    pw = 'x' * PASSWORD_MAX_LENGTH
    assert enforce_password_policy(pw) == pw


def test_rejects_short_password_with_stable_detail():
    with pytest.raises(HTTPException) as exc:
        enforce_password_policy('x' * (PASSWORD_MIN_LENGTH - 1))
    assert exc.value.status_code == 400
    assert exc.value.detail == 'password_too_short'


def test_rejects_long_password_with_stable_detail():
    with pytest.raises(HTTPException) as exc:
        enforce_password_policy('x' * (PASSWORD_MAX_LENGTH + 1))
    assert exc.value.status_code == 400
    assert exc.value.detail == 'password_too_long'


def test_rejects_non_string_with_stable_detail():
    for bad in (None, 123, {'pw': 'x'}, [], b'bytes'):
        with pytest.raises(HTTPException) as exc:
            enforce_password_policy(bad)
        assert exc.value.status_code == 400
        assert exc.value.detail == 'password_required'


def test_returns_exact_input_on_success():
    """No mutation — same string back."""
    pw = 'A_strong_PASS!123'
    assert enforce_password_policy(pw) is pw


def test_rejects_empty_string_as_too_short():
    with pytest.raises(HTTPException) as exc:
        enforce_password_policy('')
    assert exc.value.detail == 'password_too_short'
