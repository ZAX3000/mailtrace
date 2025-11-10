#!/usr/bin/env bash
set -euo pipefail
export PYTHONPATH="${PYTHONPATH:-/app}"
echo "[entrypoint] alembic upgrade head..."
alembic upgrade head || { echo "[entrypoint] Alembic migration failed"; exit 1; }
echo "[entrypoint] starting gunicorn on 0.0.0.0:${PORT:-8000}"
exec gunicorn -b 0.0.0.0:${PORT:-8000} app.wsgi:app --workers 2 --timeout 150
