# app/blueprints/auth.py
from __future__ import annotations

from functools import wraps
from typing import Any, Optional, Protocol, Callable, TypeVar, cast
import importlib

from flask import (
    Blueprint,
    current_app,
    jsonify,
    redirect,
    request,
    session,
    url_for,
)

from app.extensions import db
from app.models import User

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# ------------------------
# Protocols (type hints)
# ------------------------

class Auth0Client(Protocol):
    def authorize_redirect(self, redirect_uri: str) -> Any: ...
    def authorize_access_token(self) -> dict[str, Any]: ...
    def userinfo(self) -> dict[str, Any]: ...

class AuthClient(Protocol):
    def register(
        self,
        name: str,
        *,
        client_id: str,
        client_secret: str,
        server_metadata_url: str,
        client_kwargs: dict[str, Any],
    ) -> None: ...
    @property
    def auth0(self) -> Auth0Client: ...

# ------------------------
# Runtime adapters
# ------------------------

class _RuntimeAuth0(Auth0Client):
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def authorize_redirect(self, redirect_uri: str) -> Any:
        return self._inner.authorize_redirect(redirect_uri)

    def authorize_access_token(self) -> dict[str, Any]:
        token = self._inner.authorize_access_token()
        return dict(token) if isinstance(token, dict) else {"token": token}

    def userinfo(self) -> dict[str, Any]:
        info = self._inner.userinfo()
        return dict(info) if isinstance(info, dict) else {}

class _RuntimeAuth(AuthClient):
    def __init__(self, inner_oauth: Any) -> None:
        self._inner = inner_oauth

    def register(
        self,
        name: str,
        *,
        client_id: str,
        client_secret: str,
        server_metadata_url: str,
        client_kwargs: dict[str, Any],
    ) -> None:
        self._inner.register(
            name,
            client_id=client_id,
            client_secret=client_secret,
            server_metadata_url=server_metadata_url,
            client_kwargs=client_kwargs,
        )

    @property
    def auth0(self) -> Auth0Client:
        return _RuntimeAuth0(self._inner.auth0)

# ------------------------
# OAuth bootstrap
# ------------------------

oauth: Optional[AuthClient] = None

def _init_oauth() -> None:
    """Register Auth0 client if configured."""
    global oauth
    dom = (current_app.config.get("AUTH0_DOMAIN") or "").strip()
    cid = (current_app.config.get("AUTH0_CLIENT_ID") or "").strip()
    csec = (current_app.config.get("AUTH0_CLIENT_SECRET") or "").strip()
    if not (dom and cid and csec):
        oauth = None
        return
    try:
        m = importlib.import_module("authlib.integrations.flask_client")
        OAuth = getattr(m, "OAuth", None)
    except Exception:
        OAuth = None

    if OAuth is None:
        oauth = None
        return

    runtime = OAuth(current_app)
    client = _RuntimeAuth(runtime)
    client.register(
        "auth0",
        client_id=cid,
        client_secret=csec,
        server_metadata_url=f"https://{dom}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid profile email"},
    )
    oauth = client

@auth_bp.record_once
def _on_load(state) -> None:
    # Initialize OAuth once the blueprint is registered
    with state.app.app_context():
        _init_oauth()

# ------------------------
# Dev user helper
# ------------------------

def _ensure_dev_user() -> User:
    """
    Create or fetch a durable local dev user.
    Uses DEV_USER_EMAIL (config) or a sensible default.
    """
    email = current_app.config.get("DEV_USER_EMAIL", "dev@mailtrace.local")
    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(email=email, full_name="Dev User", provider="dev")
        db.session.add(user)
        db.session.commit()
    return user

def _dev_autologin_if_configured() -> None:
    """
    In dev (DISABLE_AUTH=1), ensure session has a valid user.
    No-op in prod.
    """
    if not current_app.config.get("DISABLE_AUTH"):
        return
    if "user_id" in session:
        return
    # Only trust local access for the silent autologin behavior
    if request.remote_addr in {"127.0.0.1", "::1"}:
        u = _ensure_dev_user()
        session["user_id"] = str(u.id)
        session["email"] = u.email

# Run for every request (blueprint-scoped, applies app-wide)
@auth_bp.before_app_request
def _attach_user_and_dev_autologin():
    _dev_autologin_if_configured()
    # (Optionally expose on g if you prefer; session is sufficient for now.)

# ------------------------
# Decorators
# ------------------------

F = TypeVar("F", bound=Callable[..., Any])

def login_required(fn: F) -> F:
    """
    For web routes: redirect to /auth/login when unauthenticated.
    In dev with DISABLE_AUTH=1, a dev user is automatically created/logged in.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            if current_app.config.get("DISABLE_AUTH"):
                # Ensure dev user (in case before_request didn't run—e.g., tests)
                u = _ensure_dev_user()
                session["user_id"] = str(u.id)
                session["email"] = u.email
            else:
                nxt = request.full_path or request.path
                return redirect(url_for("auth.login", next=nxt))
        return fn(*args, **kwargs)
    return cast(F, wrapper)

def api_login_required(fn: F) -> F:
    """
    For API routes: return JSON 401 (no redirects) when unauthenticated.
    In dev with DISABLE_AUTH=1, a dev user is automatically created/logged in.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            if current_app.config.get("DISABLE_AUTH"):
                u = _ensure_dev_user()
                session["user_id"] = str(u.id)
                session["email"] = u.email
            else:
                return jsonify({"error": "unauthorized", "message": "Please sign in to continue."}), 401
        return fn(*args, **kwargs)
    return cast(F, wrapper)

# ------------------------
# Routes
# ------------------------

@auth_bp.get("/login")
def login():
    # Dev bypass
    if current_app.config.get("DISABLE_AUTH"):
        u = _ensure_dev_user()
        session["user_id"] = str(u.id)
        session["email"] = u.email
        return redirect(request.args.get("next") or "/")

    # Real OAuth
    if oauth is None:
        return jsonify({"error": "OAuth not configured"}), 503

    redirect_uri = url_for("auth.callback", _external=True)
    return oauth.auth0.authorize_redirect(redirect_uri)

@auth_bp.get("/callback")
def callback():
    if oauth is None:
        return jsonify({"error": "OAuth not configured"}), 503

    token = oauth.auth0.authorize_access_token()
    userinfo = token.get("userinfo") or oauth.auth0.userinfo()
    email = (userinfo or {}).get("email")
    name = (userinfo or {}).get("name", "")

    if not email:
        return jsonify({"error": "No email from identity provider"}), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(email=email, full_name=name, provider="auth0")
        db.session.add(user)
        db.session.commit()

    session["user_id"] = str(user.id)
    session["email"] = user.email
    return redirect(request.args.get("next") or "/")

@auth_bp.get("/logout")
def logout():
    session.clear()
    # If you want to trigger Auth0 logout later, redirect to AUTH0_LOGOUT_URL
    return redirect(current_app.config.get("AUTH0_LOGOUT_URL") or "/")

# Small diagnostics/helpers (optional)

@auth_bp.get("/me")
def me():
    """Who am I (for debugging UI)?"""
    if "user_id" not in session:
        return jsonify({"authenticated": False})
    return jsonify({
        "authenticated": True,
        "user_id": session.get("user_id"),
        "email": session.get("email"),
        "dev": bool(current_app.config.get("DISABLE_AUTH")),
    })