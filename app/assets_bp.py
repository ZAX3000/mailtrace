import os
from flask import Blueprint, current_app, send_from_directory

assets_bp = Blueprint("assets_bp", __name__)

def _static_dir(*parts):
    return os.path.join(current_app.root_path, "static", *parts)

@assets_bp.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory(_static_dir("assets"), filename)

@assets_bp.route("/assets_v47_fix/<path:filename>")
def assets_v47_fix(filename):
    return send_from_directory(_static_dir("assets_v47_fix"), filename)
