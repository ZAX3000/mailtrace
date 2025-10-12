# MailTrace — Minimal Handoff

This package contains the source code (Flask) and database migrations (Alembic) for the MailTrace app with Mapbox→Leaflet fallback.

## What’s in here

```
app/                 # Flask app: blueprints, templates, static
  ├─ templates/      # HTML templates (dashboard, map, uploads, etc.)
  ├─ static/         # CSS/JS/images used by templates
  ├─ matching.py     # Matching logic
  ├─ dashboard_routes.py, mapview.py, api_jobs.py, ...  # Feature blueprints
alembic/             # Alembic migrations (active)
alembic.ini          # Alembic config (points at `alembic/`)
requirements.txt     # Python dependencies
Procfile             # gunicorn entrypoint (Heroku/Render-style)
runtime.txt          # Python runtime hint
.env.example         # Example environment variables

run_local.*          # Convenience scripts
README.md            # Turnkey deployment notes
.gitignore           # Keeps venv, DBs, caches out of version control
```

**Not included**: virtual environments (`.venv/`), local SQLite DBs (`instance/local.db`), compiled caches (`__pycache__`), duplicate/legacy `migrations/` (use `alembic/`).

## Running locally

```bash
# 1) Create venv & install deps
python -m venv .venv && source .venv/bin/activate     # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt

# 2) Environment
cp .env.example .env    # fill in keys as needed; MAPBOX_TOKEN may be empty to trigger Leaflet fallback

# 3) DB & migrations
alembic upgrade head    # sets up schema

# 4) Run
export FLASK_APP=app.wsgi:app && flask run            # or: gunicorn app.wsgi:app
```

## Deploy notes

- **Alembic**: `alembic.ini` is configured to use the `alembic/` directory. Remove legacy `migrations/` to avoid confusion.
- **Map**: If `MAPBOX_TOKEN` is not set, the map page uses **Leaflet via CDN** with OSM tiles (no local tiles bundled).
- **Secrets**: Fill the `.env` (or platform env vars) for Auth0, Stripe, Postgres, S3, etc.

## Handoff checklist

- [ ] Confirm target platform (Azure App Service (Containers)). Keep **only** the relevant files (e.g., keep `.platform/` only for EB).
- [ ] Add your logo/assets under `app/static/`.
- [ ] Review `matching.py` thresholds/penalties to match business rules.
- [ ] Ensure README and this handbook reflect your environment.


**Note:** On App Service with Docker, the container entrypoint runs `alembic upgrade head` automatically.
