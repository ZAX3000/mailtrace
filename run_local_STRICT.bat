@echo off
setlocal ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION
echo === MailTrace strict local runner ===
set "ROOT=%~dp0"
pushd "%ROOT%"
echo ROOT: %ROOT%
echo.

REM ----- 0) Hard sandbox so we only use local site-packages -----
set "PATH=%ROOT%.venv\Scripts;%SystemRoot%\System32;%SystemRoot%"
set "PYTHONNOUSERSITE=1"
set "PYTHONHOME="
set "PYTHONPATH=%ROOT%"
set "PIP_USER=0"

REM ----- 1) Find a Python interpreter -----
set "PYEXE="

REM Prefer Windows Python Launcher 'py -3' if available
where /Q py
if %ERRORLEVEL%==0 (
  py -3 -c "import sys; print(sys.version)" 1>nul 2>nul
  if %ERRORLEVEL%==0 (
    set "PYEXE=py -3"
  )
)

REM If still empty, try 'python' on PATH
if not defined PYEXE (
  where /Q python
  if %ERRORLEVEL%==0 (
    python -c "import sys; print(sys.version)" 1>nul 2>nul && set "PYEXE=python"
  )
)

REM Try common install locations
if not defined PYEXE if exist "%LocalAppData%\Programs\Python\Python313\python.exe" set "PYEXE=%LocalAppData%\Programs\Python\Python313\python.exe"
if not defined PYEXE if exist "%LocalAppData%\Programs\Python\Python312\python.exe" set "PYEXE=%LocalAppData%\Programs\Python\Python312\python.exe"
if not defined PYEXE if exist "%LocalAppData%\Programs\Python\Python311\python.exe" set "PYEXE=%LocalAppData%\Programs\Python\Python311\python.exe"
if not defined PYEXE if exist "%ProgramFiles%\Python313\python.exe" set "PYEXE=%ProgramFiles%\Python313\python.exe"
if not defined PYEXE if exist "%ProgramFiles%\Python312\python.exe" set "PYEXE=%ProgramFiles%\Python312\python.exe"
if not defined PYEXE if exist "%ProgramFiles%\Python311\python.exe" set "PYEXE=%ProgramFiles%\Python311\python.exe"
if not defined PYEXE if exist "%ProgramData%\Anaconda3\python.exe" set "PYEXE=%ProgramData%\Anaconda3\python.exe"
if not defined PYEXE if exist "%UserProfile%\Anaconda3\python.exe" set "PYEXE=%UserProfile%\Anaconda3\python.exe"

if not defined PYEXE (
  echo [fatal] No Python 3.x interpreter found.
  echo Install Python 3.11+ from https://www.python.org/downloads/windows/ and re-run this file.
  echo Be sure to check "Add python.exe to PATH" during install.
  echo.
  pause
  goto :EOF
)

echo Using interpreter: %PYEXE%
echo.

REM ----- 2) Create venv if missing -----
if not exist ".venv\Scripts\python.exe" (
  echo [setup] creating .venv ...
  %PYEXE% -m venv .venv
  if errorlevel 1 (
    echo [fatal] Failed to create .venv using: %PYEXE%
    echo Try installing Python 3.11+, then re-run.
    echo.
    pause
    goto :EOF
  )
)

REM After venv exists, use ONLY the venv interpreter for everything
set "VPY=%ROOT%.venv\Scripts\python.exe"
echo Using venv: %VPY%
echo.

REM ----- 3) Upgrade pip and install requirements -----
"%VPY%" -m pip install -U pip
if exist requirements.txt "%VPY%" -m pip install -r requirements.txt

REM ----- 4) Ensure instance/ and a local .env for dev (SQLite) -----
if not exist instance mkdir instance
if not exist .env (
  > .env echo DATABASE_URL=sqlite:///instance/local.db
  >> .env echo MAPBOX_TOKEN=
)

REM ----- 5) DB migrate (use local venv) -----
rem --- ensure absolute SQLite path to avoid 'unable to open database file' ---
set "CURROOT=%CD%"
if not exist "instance" mkdir "instance"
set "ABS_DB=%CURROOT%\instance\local.db"
rem Normalize backslashes to slashes for SQLAlchemy URL
set "ABS_DB=%ABS_DB:\=/%"
set "DATABASE_URL=sqlite:///%ABS_DB%"
echo [db] alembic upgrade head
"%VPY%" -m alembic upgrade head || goto dbfail

REM ----- 6) Run Flask (use local venv) -----
set "FLASK_APP=app.wsgi:app"
set "PYTHONUNBUFFERED=1"
echo [run] http://127.0.0.1:8000  (Ctrl+C to stop)
"%VPY%" -m flask run --host 127.0.0.1 --port 8000
goto end

:dbfail
echo.
echo Alembic migration FAILED. Check .env (DATABASE_URL) and alembic\env.py.
echo For local dev, keep: DATABASE_URL=sqlite:///instance/local.db
pause

:end
echo.
pause
popd
endlocal
