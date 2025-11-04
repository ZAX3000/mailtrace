# app/__init__.py
from __future__ import annotations

import os
import logging
from contextvars import ContextVar
from typing import Optional, TYPE_CHECKING, Any

from flask import Flask, send_from_directory
from dotenv import load_dotenv

from .config import Config
from .extensions import db, migrate
from .typing_ext import MailTraceFlask
from .errors import register_error_handlers

if TYPE_CHECKING:
    CORS: Any

try:
    import importlib
    _mod = importlib.import_module("flask_cors")
    CORS = getattr(_mod, "CORS", None)
except Exception:
    CORS = None

load_dotenv()

_request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


def create_app() -> Flask:
    app = MailTraceFlask(
        __name__,
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        instance_relative_config=True,
    )
    app.config.from_object(Config)
    os.makedirs(app.instance_path, exist_ok=True)

    # ---- Logging (dev-friendly)
    _configure_logging(app)

    # ---- Extensions
    db.init_app(app)
    migrate.init_app(app, db)
    register_error_handlers(app)

    # ---- Dev CORS (only if module present and we’re in dev)
    if app.config.get("ENV") != "production" and CORS:
        CORS(app, supports_credentials=True)

    # ---- Storage (lazy import to avoid heavy deps at import time)
    from .services.storage import LocalStorage
    uploads_root = os.path.join(app.instance_path, "uploads")
    app.storage = LocalStorage(uploads_root)

    # ---- Blueprints
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


def _configure_logging(app: Flask) -> None:
    """Set a simple, readable log format and DEBUG level in dev."""
    root = logging.getLogger()
    # If gunicorn/uwsgi injects handlers, avoid duplicating
    if not root.handlers:
        handler = logging.StreamHandler()
        fmt = (
            "%(asctime)s %(levelname)s "
            "[rid:%(request_id)s] "
            "%(name)s: %(message)s"
        )
        handler.setFormatter(_RequestIdFormatter(fmt))
        root.addHandler(handler)

    # Level: DEBUG in dev, INFO otherwise
    level = logging.DEBUG if app.config.get("ENV") != "production" else logging.INFO
    root.setLevel(level)
    # Quiet overly chatty loggers if needed
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


class _RequestIdFormatter(logging.Formatter):
    """Inject request-id from contextvars into log records."""
    def format(self, record: logging.LogRecord) -> str:
        rid = _request_id.get()
        # attach attribute for format string
        setattr(record, "request_id", rid or "-")
        return super().format(record)