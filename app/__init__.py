# app/__init__.py
from __future__ import annotations

import os
import click
from flask import Flask, redirect, url_for, send_from_directory

from .config import Config
from .extensions import db, migrate

# Blueprints
from .blueprints.dashboard_routes import dashboard_bp
from .blueprints.api_jobs import api_bp
from .blueprints.runs import runs_bp
from .blueprints.auth import auth_bp
from .blueprints.billing import billing_bp
from .blueprints.map import map_bp
from .blueprints.health import health_bp

from .services.storage import LocalStorage
from .services.map_cache import build_map_cache            # <- import directly; no try/except
from .typing_ext import MailTraceFlask                     # <- typed Flask subclass

def register_cli(app: Flask) -> None:
    @app.cli.command("build-map-cache")
    @click.option("--limit", default=1000, show_default=True, help="Max points to include")
    def build_map_cache_cmd(limit: int):
        """Build the cached GeoJSON used by /map/data."""
        with app.app_context():
            path = build_map_cache(limit=limit)            # <- now matches signature
            click.echo(f"Wrote {path}")

def create_app() -> Flask:
    app = MailTraceFlask(                                  # <- use typed subclass
        __name__,
        static_folder=os.path.join(os.path.dirname(__file__), "static"),
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
        instance_relative_config=True,
    )
    app.config.from_object(Config)

    os.makedirs(app.instance_path, exist_ok=True)

    db.init_app(app)
    migrate.init_app(app, db)

    uploads_root = os.path.join(app.instance_path, "uploads")
    app.storage = LocalStorage(uploads_root)               # <- mypy now happy

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(runs_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(billing_bp)
    app.register_blueprint(map_bp)
    app.register_blueprint(health_bp)

    register_cli(app)

    @app.route("/")
    def index():
        return redirect(url_for("dashboard.index"))

    @app.route("/favicon.ico")
    def favicon():
        static_root = os.path.join(app.root_path, "static")
        return send_from_directory(static_root, "favicon.ico")

    return app