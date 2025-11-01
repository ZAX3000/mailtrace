# app/__init__.py
from __future__ import annotations

import os
from flask import Flask, redirect, url_for, send_from_directory, session, request
from dotenv import load_dotenv

from .config import Config
from .extensions import db, migrate
from .typing_ext import MailTraceFlask
from .errors import register_error_handlers

load_dotenv()

def create_app() -> Flask:
    app = MailTraceFlask(
        __name__,
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        instance_relative_config=True,
    )
    app.config.from_object(Config)
    os.makedirs(app.instance_path, exist_ok=True)

    # Extensions
    db.init_app(app)
    migrate.init_app(app, db)
    register_error_handlers(app)

    # --- Dev autologin (remove once real Auth0 is live) ---
    if app.config.get("DISABLE_AUTH"):
        @app.before_request
        def _dev_autologin():
            # Only trust local requests in dev
            if "user_id" in session:
                return
            if request.remote_addr in {"127.0.0.1", "::1"}:
                from app.blueprints.auth import _ensure_dev_user
                u = _ensure_dev_user()
                session["user_id"] = str(u.id)     # <- real UUID from DB
                session["email"] = u.email
    # -------------------------------------------------------

    # Lazy import to avoid heavy imports at module load time
    from .services.storage import LocalStorage
    uploads_root = os.path.join(app.instance_path, "uploads")
    app.storage = LocalStorage(uploads_root)

    # Register blueprints (import inside the factory)
    from .blueprints.api import api_bp
    from .blueprints.auth import auth_bp
    from .blueprints.billing import billing_bp
    from .blueprints.map import map_bp
    from .blueprints.health import health_bp

    app.register_blueprint(api_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(billing_bp)
    app.register_blueprint(map_bp)
    app.register_blueprint(health_bp)

    @app.route("/favicon.ico")
    def favicon():
        static_root = os.path.join(app.root_path, "static")
        return send_from_directory(static_root, "favicon.ico")

    return app