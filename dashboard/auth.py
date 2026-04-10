"""Authentication module for the reclip_bot admin dashboard."""
import os
from typing import Optional

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import Request, Response

COOKIE_NAME = "reclip_session"
_MAX_AGE_SECONDS = 86400  # 24 hours


def _admin_user() -> str:
    return os.environ.get("ADMIN_USER", "admin")


def _admin_password() -> str:
    pw = os.environ.get("ADMIN_PASSWORD")
    if not pw:
        raise RuntimeError("ADMIN_PASSWORD environment variable is required")
    return pw


def _secret_key() -> str:
    return os.environ.get("SECRET_KEY", "insecure-default-key-change-me")


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(_secret_key())


def verify_credentials(username: str, password: str) -> bool:
    """Return True if credentials match env-configured admin user/password."""
    return username == _admin_user() and password == _admin_password()


def create_session_cookie(response: Response, username: str) -> None:
    """Sign and set the session cookie on the response."""
    token = _serializer().dumps(username)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
    )


def get_current_user(request: Request) -> Optional[str]:
    """Read the session cookie and return the username, or None if invalid/missing."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    try:
        username = _serializer().loads(token, max_age=_MAX_AGE_SECONDS)
        return username
    except (BadSignature, SignatureExpired):
        return None
