import os
from flask import Blueprint, current_app, send_from_directory

assets_bp = Blueprint("assets_bp", __name__)

def _root():
    return current_app.static_folder

@assets_bp.route("/assets/<path:filename>")
def assets(filename):
    return send_from_directory(os.path.join(_root(), "assets"), filename)

@assets_bp.route("/assets_v47_fix/<path:filename>")
def assets_fix(filename):
    return send_from_directory(os.path.join(_root(), "assets_v47_fix"), filename)
