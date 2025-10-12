from .assets_proxy import assets_bp
import os
from flask import Flask, redirect, url_for, send_from_directory
from .config import Config
from .extensions import db, migrate
from .dashboard_routes import dashboard_bp
from .api_jobs import api_bp
from .runs import runs_bp
from .auth import auth_bp
from .billing import billing_bp
from .mapview import map_bp
from .health import health_bp
from .assets_bp import assets_bp

def create_app():
    app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), "static"))
    app.config.from_object(Config)

    # init extensions
    db.init_app(app)
    migrate.init_app(app, db)

    # blueprints
    app.register_blueprint(assets_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(runs_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(billing_bp)
    app.register_blueprint(map_bp)
    app.register_blueprint(health_bp)

    # default index -> dashboard
    @app.route("/")
    def index():
        return redirect(url_for("dashboard.index"))

    # favicon fallback
    @app.route("/favicon.ico")
    def favicon():
        static_root = os.path.join(app.root_path, "static")
        return send_from_directory(static_root, "favicon.ico")

    return app
