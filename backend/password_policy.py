"""Phase 2.18Z — password policy.

Extracted from server.py so signup, password change, password reset,
and any future flow share one source of truth. The function raises
FastAPI's HTTPException so callers can just call it inline and let
the framework translate it to a 400 response. Detail strings are
stable identifiers the frontend already translates.
"""
from __future__ import annotations

from fastapi import HTTPException


# Phase 2.18G: previously the signup path had no length check at all
# (a 1-character password was accepted), while password change/reset
# required 6 characters. One central policy fixes both gaps.
PASSWORD_MIN_LENGTH = 8
PASSWORD_MAX_LENGTH = 256


def enforce_password_policy(candidate: object) -> str:
    """Validate a new password. Raises HTTPException(400, ...) with a
    stable detail string the frontend already translates. Returns the
    validated string on success.

    Identifiers used by the frontend:
        password_required   - not a string / missing
        password_too_short  - shorter than PASSWORD_MIN_LENGTH
        password_too_long   - longer than PASSWORD_MAX_LENGTH
    """
    if not isinstance(candidate, str):
        raise HTTPException(400, 'password_required')
    if len(candidate) < PASSWORD_MIN_LENGTH:
        raise HTTPException(400, 'password_too_short')
    if len(candidate) > PASSWORD_MAX_LENGTH:
        raise HTTPException(400, 'password_too_long')
    return candidate
