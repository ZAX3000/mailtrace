
# MailTrace Cleanup & Server-Rendered Dashboard

## Changes
- Switched to server-rendered dashboard: `/dashboard` now renders `app/templates/dashboard.html`.
- Home `/` redirects to `/dashboard`.
- `app/templates/dashboard.html` is based on the former static `app/static/dashboard.html`, with a small Jinja context injection (`window.MT_CONTEXT.run_id`).
- Duplicate route in `app/runs.py`: second `GET /upload` changed to `GET /list`. POST flow now redirects to `/dashboard?run_id=...` and stores `session['last_run_id']`.
- Registered a simple `/healthz` endpoint.
- Removed legacy/unused files: `app/dashboard.py`, `app/dashboard_export_v18.py`, `app/score_units_patch.py`.
- Removed bloat from repo/package: `.venv/`, `runlog.txt`, `requirements-lite.txt`.

## How data flows
1. User visits `/runs/upload` to submit the two CSVs.
2. `POST /runs/upload` runs matching, persists run + optional geocodes, and redirects to `/dashboard`.
3. The dashboard template uses your existing JS to fetch data (via `/api` endpoints). It also sees `window.MT_CONTEXT.run_id` if present.

## Notes
- If you want to keep a runs list page, it is now at `/runs/list`.
- Ensure env vars for Auth0, Stripe, DB, and Mapbox are set per `.env.example`.
- Consider excluding `.venv/` and local `.bat` scripts from production deploys.
