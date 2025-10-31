# app/blueprints/auth.py
from __future__ import annotations

import uuid
import importlib
from functools import wraps
from typing import Any, Optional, Protocol

from flask import (
    Blueprint,
    Flask,
    current_app,
    jsonify,
    redirect,
    request,
    session,
    url_for,
)

from ..extensions import db
from ..models import User


# ---------- Typed interface (what the rest of the app relies on) ----------

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


# ---------- Runtime adapter (no third-party types leak into our code) ----------

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
        # Authlib exposes `oauth.auth0` after `register`
        return _RuntimeAuth0(self._inner.auth0)


# ---------- Blueprint setup ----------

auth_bp = Blueprint("auth", __name__)
oauth: Optional[AuthClient] = None


# ---------- Internal helpers ----------

def _init_oauth(app: Flask) -> None:
    """
    Register Auth0 only if creds exist (skipped in dev with DISABLE_AUTH=1).
    Leaves `oauth` as None if not configured or if Authlib is unavailable.
    """
    global oauth

    dom = (app.config.get("AUTH0_DOMAIN") or "").strip()
    cid = (app.config.get("AUTH0_CLIENT_ID") or "").strip()
    csec = (app.config.get("AUTH0_CLIENT_SECRET") or "").strip()
    if not (dom and cid and csec):
        return  # not configured -> bypass (e.g., local dev)

    try:
        m = importlib.import_module("authlib.integrations.flask_client")
        OAuthRuntime = getattr(m, "OAuth", None)
    except Exception:
        OAuthRuntime = None

    if OAuthRuntime is None:
        return

    runtime = OAuthRuntime(app)
    oauth_runtime = _RuntimeAuth(runtime)
    oauth_runtime.register(
        "auth0",
        client_id=cid,
        client_secret=csec,
        server_metadata_url=f"https://{dom}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid profile email"},
    )
    oauth = oauth_runtime


@auth_bp.record_once
def _on_load(state) -> None:
    _init_oauth(state.app)


def _ensure_dev_user() -> User:
    """Create or get a durable local dev user with a stable UUID."""
    from flask import current_app
    email = "dev@local.test"
    dev_id = current_app.config.get("DEV_USER_ID")
    user = db.session.query(User).filter_by(email=email).first()
    if not user:
        user = User(id=dev_id, email=email, full_name="Dev User", provider="dev")
        db.session.add(user)
        db.session.commit()
    return user


# ---------- Public decorator ----------
def login_required(f):
    """
    Bypass auth locally (DISABLE_AUTH=1) but still provide a valid user_id UUID.
    Otherwise, require an authenticated session.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        if current_app.config.get("DISABLE_AUTH"):
            if "user_id" not in session:
                u = _ensure_dev_user()
                session["user_id"] = str(u.id)
                session["email"] = u.email
            return f(*args, **kwargs)

        if "user_id" not in session:
            return redirect(url_for("auth.login", next=request.full_path))
        return f(*args, **kwargs)

    return wrapper


# ---------- Routes ----------

@auth_bp.get("/login")
def login():
    # Dev bypass
    if current_app.config.get("DISABLE_AUTH"):
        u = _ensure_dev_user()
        session["user_id"] = str(u.id)
        session["email"] = u.email
        return redirect(request.args.get("next") or url_for("dashboard.index"))

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
        return jsonify({"error": "No email in identity"}), 400

    user = db.session.query(User).filter_by(email=email).first()
    if not user:
        user = User(email=email, full_name=name, provider="auth0")
        db.session.add(user)
        db.session.commit()

    session["user_id"] = str(user.id)
    session["email"] = user.email
    return redirect(request.args.get("next") or url_for("dashboard.index"))


@auth_bp.get("/logout")
def logout():
    session.clear()
    return redirect(current_app.config.get("AUTH0_LOGOUT_URL") or "/")