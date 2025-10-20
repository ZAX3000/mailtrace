# app/blueprints/auth.py
from __future__ import annotations

import uuid
from functools import wraps
from flask import Blueprint, current_app, redirect, request, session, url_for, jsonify
from ..extensions import db
from ..models import User

# Authlib is optional in dev; guard the import
try:
    from authlib.integrations.flask_client import OAuth  # type: ignore
except Exception:  # pragma: no cover
    OAuth = None  # type: ignore

auth_bp = Blueprint("auth", __name__)
oauth = None  # set in _init_oauth when configured


# --- Internal helpers -------------------------------------------------------

def _init_oauth(app):
    """Register Auth0 only if creds exist (skipped in dev with DISABLE_AUTH=1)."""
    global oauth
    if OAuth is None:
        return
    dom  = (app.config.get("AUTH0_DOMAIN") or "").strip()
    cid  = (app.config.get("AUTH0_CLIENT_ID") or "").strip()
    csec = (app.config.get("AUTH0_CLIENT_SECRET") or "").strip()
    if not (dom and cid and csec):
        return  # not configured -> bypass in dev
    oauth = OAuth(app)
    oauth.register(
        "auth0",
        client_id=cid,
        client_secret=csec,
        server_metadata_url=f"https://{dom}/.well-known/openid-configuration",
        client_kwargs={"scope": "openid profile email"},
    )


@auth_bp.record_once
def _on_load(state):
    _init_oauth(state.app)


def _ensure_dev_user() -> User:
    """Create or get a durable local dev user with a real UUID."""
    email = "dev@local.test"
    user = db.session.query(User).filter_by(email=email).first()
    if not user:
        user = User(id=uuid.uuid4(), email=email, full_name="Dev User", provider="dev")
        db.session.add(user)
        db.session.commit()
    return user


# --- Public decorator --------------------------------------------------------

def login_required(f):
    """Bypass auth locally (DISABLE_AUTH=1) but still provide a valid user_id UUID."""
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


# --- Routes -----------------------------------------------------------------

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
    name  = (userinfo or {}).get("name", "")

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
