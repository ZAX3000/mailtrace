# First run on Windows

Use **run_local_STRICT.bat**. It:
- Finds a local Python (tries `py -3`, `python`, and common install folders)
- Creates `.venv` if missing, then uses only `.\.venv\Scripts\python.exe`
- Seeds `.env` with `DATABASE_URL=sqlite:///instance/local.db`
- Runs `alembic upgrade head` and starts Flask on http://127.0.0.1:8000

If it prints **"[fatal] No Python 3.x interpreter found."**:
1. Install Python 3.11+ from python.org (Windows) and check **"Add python.exe to PATH"**.
2. Re-run `run_local_STRICT.bat`.
