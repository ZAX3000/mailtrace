import os
from flask import Blueprint, current_app, send_from_directory, abort, Response

assets_bp = Blueprint("assets_bp", __name__)

def _safe_send(root, filename):
    # Normalize and prevent path traversal
    full_root = os.path.abspath(root)
    path = os.path.abspath(os.path.join(full_root, filename))
    if not path.startswith(full_root):
        abort(404)
    if os.path.exists(path):
        return send_from_directory(full_root, filename)
    # Minimal placeholders to avoid console noise
    if filename.endswith(".css"):
        return Response("", mimetype="text/css")
    if filename.endswith(".js"):
        return Response("/* placeholder */", mimetype="application/javascript")
    return abort(404)

@assets_bp.route("/assets/<path:filename>")
def assets(filename):
    root = os.path.abspath(os.path.join(current_app.root_path, "..", "assets"))
    return _safe_send(root, filename)

@assets_bp.route("/assets_v47_fix/<path:filename>")
def assets_v47(filename):
    root = os.path.abspath(os.path.join(current_app.root_path, "..", "assets_v47_fix"))
    return _safe_send(root, filename)
