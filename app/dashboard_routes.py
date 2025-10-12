
from flask import Blueprint, render_template, request, session
from .auth import login_required

dashboard_bp = Blueprint("dashboard", __name__)

@dashboard_bp.get("/dashboard")
@login_required
def dashboard_view():
    # Optionally pass a specific run_id for the template's JS to fetch
    run_id = request.args.get("run_id") or session.get("last_run_id")
    return render_template("dashboard.html", run_id=run_id)
