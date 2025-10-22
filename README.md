# MailTrace – Developer Setup (Postgres dev parity)

This guide takes you from a clean machine to a running dev server using **PostgreSQL in Docker** so dev/CI/prod match. Windows-first examples are shown, with macOS/Linux notes where helpful.
The system is now prepped for an Azure setup alongside Docker dev server.

---

## 0) Prerequisites

* **Git**
* **Python 3.11+** (3.12/3.13 OK)
* **Docker Desktop** (WSL2 backend on Windows)
* (Optional) **pgAdmin** or **DBeaver** for browsing the DB
* **VS Code** with:

  * *Jinja* or *Better Jinja* (syntax highlight)
  * Python extension

> VS Code tip – add `.vscode/settings.json`
>
> ```json
> {
>   "files.associations": {
>     "**/app/templates/**/*.html": "jinja-html",
>     "*.jinja": "jinja-html",
>     "*.j2": "jinja-html"
>   },
>   "html.format.templating": true,
>   "html.validate.scripts": false
> }
> ```

---

## 0.5) Cold boot after a reboot (TL;DR)

1. **Start Docker Desktop** → wait for **Engine running** (enable auto-start in Settings → General).
2. In the repo:

   ```bash
   docker compose up -d db
   ```
3. Activate venv & env, run migrations (safe to re-run), start Flask:

   ```bash
   # Windows Git Bash/MSYS2
   source .venv/Scripts/activate
   python -m flask --app app:create_app db upgrade
   python -m flask --app app:create_app run --debug -h 127.0.0.1 -p 5000
   ```
4. Open [http://127.0.0.1:5000/](http://127.0.0.1:5000/)

> If you see `open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified`,
> Docker Engine isn’t running. Start Docker Desktop, then retry `docker compose up -d`.

---

## 1) Clone and enter the project

```bash
git clone <your-fork-or-origin> mailtrace
cd mailtrace
```

---

## 2) Create & activate a virtual environment

### Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Windows (Git Bash/MSYS2)

```bash
python -m venv .venv
source .venv/Scripts/activate
```

### macOS/Linux

```bash
python -m venv .venv
source .venv/bin/activate
```

Upgrade pip (recommended):

```bash
python -m pip install --upgrade pip
```

---

## 3) Install Python dependencies

```bash
pip install -r requirements.txt
# If psycopg isn't pulled by your resolver:
pip install "psycopg[binary]"
```

---

## 4) Start Postgres in Docker

The repo includes `docker-compose.yml` with a `db` service.

* Container **5432** → host **5433**
* Credentials: `mailtrace` / `devpass`
* DB name: `mailtrace`

Start it:

```bash
docker compose up -d db
docker compose ps      # expect mailtrace-db healthy on 0.0.0.0:5433->5432/tcp
```

Reset (fresh DB):

```bash
docker compose down -v && docker compose up -d db
```

> Windows note: Docker Desktop must be running. If `docker compose` errors with the pipe path, open Docker Desktop and try again.

---

## 5) Configure environment

Create `.env` (or copy from `.env.example`) with **Postgres as default**:

```env
# Flask (Flask 3: use --debug flag at runtime; FLASK_ENV is deprecated)
FLASK_APP=app:create_app
DISABLE_AUTH=1

# Database (dev/CI/prod parity = Postgres)
DATABASE_URL=postgresql+psycopg://mailtrace:devpass@localhost:5433/mailtrace
SQLALCHEMY_DATABASE_URI=${DATABASE_URL}
```

---

## 6) Apply database migrations (Alembic/Flask-Migrate)

```bash
python -m flask --app app:create_app db upgrade
```

**Verify (optional):**

```bash
python - <<'PY'
from sqlalchemy import create_engine, text
e = create_engine("postgresql+psycopg://mailtrace:devpass@127.0.0.1:5433/mailtrace")
with e.begin() as c:
    print(c.execute(text("select current_user, current_database()")).fetchone())
PY
```

---

## 7) Run the app

```bash
python -m flask --app app:create_app run --debug -h 127.0.0.1 -p 5000
```

Open [http://127.0.0.1:5000/](http://127.0.0.1:5000/) and walk the main pages.

---

## 8) Connect with a DB client (optional)

* Host: `127.0.0.1`
* Port: `5433`
* Database: `mailtrace`
* User: `mailtrace`
* Password: `devpass`
* SSL: off

---

## 9) Common tasks

### Create a new migration after model changes

```bash
python -m flask --app app:create_app db migrate -m "describe change"
python -m flask --app app:create_app db upgrade
```

### Reset the local DB completely

```bash
docker compose down -v
docker compose up -d db
python -m flask --app app:create_app db upgrade
```

### Seed dev data (optional)

Create `scripts/seed_dev.py`, then:

```bash
python scripts/seed_dev.py
```

---

## 10) Troubleshooting

* **Docker engine not running (Windows)**
  Error: `open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified`
  **Fix:** Start Docker Desktop → wait for **Engine running** → `docker compose up -d`.

* **`psycopg OperationalError: connection refused`**
  `docker compose ps` → ensure `mailtrace-db` is **healthy**. Port must be **5433** on host.

* **`password authentication failed for user "mailtrace"`**
  `.env` must match docker creds. If changed, `docker compose down -v && docker compose up -d db`.

* **Nothing at [http://127.0.0.1:5000/](http://127.0.0.1:5000/)**
  App may have crashed on first DB call. Check Flask terminal for traceback; ensure DB is up and healthy.

* **Flask dev flags**
  Use `--debug` with Flask 3. `FLASK_ENV` is deprecated and ignored.

---

## 11) What’s in this setup (and why)

* **Postgres in Docker** on **5433** → local/dev/CI mirrors prod behavior.
* **Flask-Migrate/Alembic** manages schema changes.
* **`.env`** is the single source of truth for DB config.
* **Jinja** template mapping in VS Code to avoid false lint errors.

---

### Appendix A — Project layout

```
├─ app/                 # create_app(), routes, services, repositories, models, templates, static
├─ alembic/             # migrations
├─ docker-compose.yml   # postgres:16 on host 5433
├─ requirements.txt
├─ .env.example
└─ README.md
```

### Appendix B — Quick demo (no login)

1. `docker compose up -d db`
2. `python -m flask --app app:create_app db upgrade`
3. `.env: DISABLE_AUTH=1`
4. `python -m flask --app app:create_app run --debug`
5. Open [http://127.0.0.1:5000/](http://127.0.0.1:5000/)
6. Upload the two sample spreadsheets from `app/static/samples/`.

### Appendix C — Full reset (nuclear option)

```bash
docker compose down -v
docker compose up -d db
python -m flask --app app:create_app db upgrade
```

---

If you want, I can also add a tiny `start-dev.cmd`/`start-dev.sh` so one double-click brings up Docker → DB → Flask.