# app/dashboard_routes.py web controller for dashboard pages
from flask import Blueprint, render_template, request, session
from .auth import login_required

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/dashboard")

@dashboard_bp.get("/")
@login_required
def index():
    run_id = request.args.get("run_id") or session.get("last_run_id")
    return render_template("dashboard.html", run_id=run_id)