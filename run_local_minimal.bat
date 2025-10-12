@echo off
setlocal
cd /d "%~dp0"
echo === MailTrace local (cmd) ===

:: 1) Create venv if missing
if not exist ".venv\Scripts\activate.bat" (
  echo [setup] Creating venv...
  py -3 -m venv .venv || python -m venv .venv
)

:: 2) Activate venv
call .venv\Scripts\activate.bat

:: 3) Install deps
python -m pip install -U pip
if exist requirements.txt (
  pip install -r requirements.txt
)

:: 4) Ensure instance/ and a safe local .env (SQLite) for first run
if not exist instance mkdir instance
if not exist .env (
  > .env echo DATABASE_URL=sqlite:///instance/local.db
  >> .env echo MAPBOX_TOKEN=
)

:: 5) Env for Flask and Python path (so alembic can import app)
set FLASK_APP=app.wsgi:app
set PYTHONUNBUFFERED=1
set PYTHONPATH=%CD%

:: 6) DB migrate
echo [db] alembic upgrade head
alembic upgrade head || goto dbfail

:: 7) Run
echo [run] http://127.0.0.1:8000  (Ctrl+C to stop)
python -m flask run --host 127.0.0.1 --port 8000
goto end

:dbfail
echo.
echo Alembic migration FAILED. Check DATABASE_URL in .env
echo For local dev, keep: DATABASE_URL=sqlite:///instance/local.db
pause

:end
echo.
pause
endlocal
