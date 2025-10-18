import os, json
from functools import wraps
from flask import Blueprint, current_app, redirect, request, session, url_for, jsonify
from authlib.integrations.flask_client import OAuth
from .extensions import db
from .models import User

auth_bp = Blueprint("auth", __name__)
oauth = OAuth()

def _init_oauth(app):
    oauth.register(
        "auth0",
        client_id=app.config["AUTH0_CLIENT_ID"],
        client_secret=app.config["AUTH0_CLIENT_SECRET"],
        server_metadata_url=f'https://{app.config["AUTH0_DOMAIN"]}/.well-known/openid-configuration',
        client_kwargs={"scope": "openid profile email"},
    )

@auth_bp.record_once
def on_load(state):
    app = state.app
    _init_oauth(app)

def login_required(f):
    @wraps(f)
    def wrapper(*a, **kw):
        from flask import current_app
        if current_app.config.get("DISABLE_AUTH"):
            # auto stub a user in dev/demo
            session.setdefault("user_id", "demo-user")
            session.setdefault("email", "demo@example.com")
            return f(*a, **kw)
        if "user_id" not in session:
            return redirect(url_for("auth.login"))
        return f(*a, **kw)
    return wrapper

@auth_bp.get("/login")
def login():
    from flask import current_app
    if current_app.config.get("DISABLE_AUTH"):
        session["user_id"] = "demo-user"
        session["email"] = "demo@example.com"
        return redirect(url_for("dashboard.index"))
    redirect_uri = url_for("auth.callback", _external=True)
    return oauth.auth0.authorize_redirect(redirect_uri)

@auth_bp.get("/callback")
def callback():
    token = oauth.auth0.authorize_access_token()
    userinfo = token.get("userinfo") or oauth.auth0.userinfo()
    email = userinfo.get("email")
    name = userinfo.get("name", "")

    user = db.session.execute(db.select(User).where(User.email==email)).scalar_one_or_none()
    if not user:
        user = User(email=email, full_name=name, provider="auth0")
        db.session.add(user); db.session.commit()

    session["user_id"] = str(user.id)
    session["email"] = email
    return redirect(url_for("dashboard.index"))

@auth_bp.get("/logout")
def logout():
    session.clear()
    return redirect(current_app.config.get("AUTH0_LOGOUT_URL") or "/")
