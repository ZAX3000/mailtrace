# app/assets_bp.py  (consolidated)
from __future__ import annotations
import os
from typing import Iterable
from flask import Blueprint, current_app, send_from_directory, abort, Response

# Keep name and routes stable for callers
assets_bp = Blueprint("assets_bp", __name__)

def _candidate_roots(subdir: str) -> Iterable[str]:
    # 1) Flask static folder (usually app/static)
    if current_app.static_folder:
        yield os.path.join(current_app.static_folder, subdir)
    # 2) Explicit app/static (covers custom setups)
    yield os.path.join(current_app.root_path, "static", subdir)
    # 3) Project-root sibling (legacy/dev layout, old "proxy" behavior)
    yield os.path.abspath(os.path.join(current_app.root_path, "..", subdir))

def _safe_send_from_roots(subdir: str, filename: str):
    # Path traversal guard + multi-root lookup
    for root in _candidate_roots(subdir):
        full_root = os.path.abspath(root)
        path = os.path.abspath(os.path.join(full_root, filename))
        # ensure requested path stays within root
        if not (path == full_root or path.startswith(full_root + os.sep)):
            continue
        if os.path.exists(path):
            rel = os.path.relpath(path, full_root)
            return send_from_directory(full_root, rel)

    # Graceful fallbacks for missing common asset types (old proxy behavior)
    if filename.endswith(".css"):
        return Response("", mimetype="text/css")
    if filename.endswith(".js"):
        return Response("/* placeholder */", mimetype="application/javascript")
    abort(404)

@assets_bp.route("/assets/<path:filename>")
def assets(filename: str):
    return _safe_send_from_roots("assets", filename)

@assets_bp.route("/assets_v47_fix/<path:filename>")
def assets_v47_fix(filename: str):
    return _safe_send_from_roots("assets_v47_fix", filename)