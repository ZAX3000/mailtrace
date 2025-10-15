# MailTrace – Developer Setup (Postgres dev parity)

This guide takes you from a clean machine to a running dev server using **PostgreSQL in Docker** so dev/CI/prod match. Windows-first examples are shown, with macOS/Linux notes where helpful. 
The system is now prepped for a Azure set up alongside Docker dev server - stick with just Docker dev server for the MVP.

---

## 0) Prerequisites

* **Git**
* **Python 3.11+** (3.12/3.13 OK)
* **Docker Desktop**
* (Optional) **pgAdmin** or **DBeaver** for browsing the DB
* **VS Code** with:

  * *Jinja* or *Better Jinja* (syntax highlight)
  * Python extension

> Tip (VS Code): add `.vscode/settings.json`
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
# If psycopg isn't pulled by your pip resolver, run:
pip install "psycopg[binary]"
```

---

## 4) Start Postgres in Docker

The repo includes `docker-compose.yml` with a `db` service.

* It maps container **5432** ➜ host **5433**
* Credentials: `mailtrace` / `devpass`
* DB name: `mailtrace`

Start it:

```bash
docker compose up -d db
docker compose ps   # should show mailtrace-db healthy on 0.0.0.0:5433->5432/tcp
```

Reset (if you need a fresh DB later):

```bash
docker compose down -v && docker compose up -d db
```

---

## 5) Configure environment

Create `.env` (or copy from `.env.example`) with **Postgres as default**:

```env
# Flask
FLASK_APP=app:create_app
FLASK_ENV=development
DISABLE_AUTH=1

# Database (dev/CI/prod parity = Postgres)
DATABASE_URL=postgresql+psycopg://mailtrace:devpass@localhost:5433/mailtrace
SQLALCHEMY_DATABASE_URI=${DATABASE_URL}

# (Other provider keys/secrets can stay blank in dev)
```

> On Windows Git Bash/MSYS2 the app reads `.env` automatically (via `python-dotenv`), but exporting is also fine:
> `export DATABASE_URL='postgresql+psycopg://mailtrace:devpass@localhost:5433/mailtrace'`

---

## 6) Apply database migrations (Alembic)

Alembic is wired to read `.env` and point at Postgres. Run:

```bash
python -m alembic upgrade head
```

* On first run you’ll see something like:
  `Context impl PostgresqlImpl. Will assume transactional DDL. Running upgrade -> 0001_init ...`

> If the DB is brand new and Alembic didn’t create `alembic_version`, stamp then upgrade:
>
> ```bash
> python -m alembic stamp base
> python -m alembic upgrade head
> ```

**Verify (optional):**

```bash
python - <<'PY'
from sqlalchemy import create_engine, text
e = create_engine("postgresql+psycopg://mailtrace:devpass@127.0.0.1:5433/mailtrace")
with e.begin() as c:
    print(c.execute(text("select current_user, current_database(), version()")).fetchone())
PY
```

---

## 7) Run the app

```bash
python -m flask run --reload
```

Open [http://127.0.0.1:5000/](http://127.0.0.1:5000/) and walk the main pages (dashboard, upload, etc.).

---

## 8) Connect with a DB client (optional)

**pgAdmin / DBeaver connection details**

* Host: `127.0.0.1`
* Port: `5433`
* Database: `mailtrace`
* User: `mailtrace`
* Password: `devpass`
* SSL: off

In pgAdmin, the server name can be anything (e.g., *mailtrace (dev)*).

---

## 9) Common tasks

### Create a new migration after model changes

```bash
python -m alembic revision -m "describe change" --autogenerate
python -m alembic upgrade head
```

### Reset the local DB completely

```bash
docker compose down -v
docker compose up -d db
python -m alembic upgrade head
```

### Seed a little dev data (optional)

Create `scripts/seed_dev.py` like this and adjust table/columns to your schema:

```python
import os
from sqlalchemy import create_engine, text
url = os.getenv("DATABASE_URL", "postgresql+psycopg://mailtrace:devpass@127.0.0.1:5433/mailtrace")
e = create_engine(url)
with e.begin() as c:
    c.execute(text("/* add your INSERTs here */ SELECT 1"))
print("Seed completed.")
```

Run with `python scripts/seed_dev.py`.

---

## 10) Troubleshooting

* **`psycopg OperationalError: connection refused`**

  * `docker compose ps` → ensure `mailtrace-db` is healthy.
  * Port must be **5433** on host.
  * Try `127.0.0.1` instead of `localhost`.

* **`password authentication failed for user "mailtrace"`**

  * Ensure `.env` matches the docker credentials.
  * If you changed creds, reset the volume: `docker compose down -v && docker compose up -d db`.

* **`alembic_version does not exist`**

  * Run:

    ```bash
    python -m alembic stamp base
    python -m alembic upgrade head
    ```
  * Make sure `.env` is loaded (you should see the printed `ALEMBIC URL = ...:5433/...` if your `env.py` echoes it).

* **VS Code shows thousands of red JS errors in Jinja templates**

  * Use the `.vscode/settings.json` above so templates are treated as **Jinja**, not plain HTML/JS.
  * Keep real JS/TS in `.js/.ts` files for proper linting.

---

## 11) What’s in this setup (and why)

* **Postgres in Docker** on port **5433** → local/dev/CI mirrors production behavior.
* **Alembic** manages schema changes; initial revision is a safe stub, app models create tables.
* **`.env`** is the single source of truth for DB config.
* **Jinja templates** with VS Code mapping to avoid false lint errors.

---

That’s it—you’re in a production-like dev environment with Postgres, migrations, and hot reload.

---

1. **Appendix: Project layout**
   Short map so new devs know where things live.

   ```md
   ## Appendix A — Project layout

   ├─ app/
   │  ├─ __init__.py            # create_app(), blueprint registration
   │  ├─ extensions.py          # db, migrate, other Flask extensions
   │  ├─ models/                # SQLAlchemy models (imported by Alembic)
   │  ├─ services/              # business logic (file parsing, matching, metrics)
   │  ├─ repositories/          # DB-access helpers (queries)
   │  ├─ routes/                # Flask blueprints (thin controllers)
   │  ├─ templates/             # Jinja2 templates (dashboard, uploads, etc.)
   │  └─ static/                # JS/CSS/assets
   ├─ alembic/
   │  ├─ env.py                 # reads .env, targets Postgres, imports models
   │  └─ versions/              # migration scripts (0001_init etc.)
   ├─ docker-compose.yml        # postgres:16 on host 5433
   ├─ requirements.txt
   ├─ .env.example              # Postgres-first defaults
   ├─ scripts/                  # utilities: seeding, one-off tools
   └─ README.md                 # this file
   ```

2. **Appendix: Click‑By‑Click Demo (No Tech Skills Needed)**
   A tiny “how to click around” for PMs/stakeholders.

   ```md
   ## Appendix B — Quick demo (no login)
    1) Start DB: `docker compose up -d db`  
    2) Migrate: `python -m alembic upgrade head`  
    3) Disable auth: set `DISABLE_AUTH=1` in your `.env`  
    4) Run: `python -m flask run --reload`  
    5) Open http://127.0.0.1:5000/  
    6) Go to **Upload** → select the two sample spreadsheets (CRM + Addresses) from `app/static/samples/`.  
    7) Watch the dashboard populate (KPIs, charts, results table).  
    8) Use filters / city/zip links to explore.
   ```

3. **Changelog (recent)**
   So readers know why things changed and where to look.

   ```md
   ## Changelog (recent)
   - Switched local/dev to **Postgres (Docker, 5433)** for prod parity.
   - Consolidated duplicated READMEs into this single guide.
   - Alembic `env.py` loads `.env`, imports models, and targets `public` version table.
   - VS Code mapping for Jinja to silence bogus JS errors in templates.
   ```

4. **Alembic notes (first-run gotchas)**

   ```md
   ### Alembic notes
   - First revision `0001_init` is a **stub**; tables come from SQLAlchemy models.
   - `alembic/env.py` imports `app.models/*` and calls `db.metadata.create_all()` to ensure tables exist.
   - If you ever see UUID errors, run once: `CREATE EXTENSION IF NOT EXISTS pgcrypto;`
   ```

5. **Windows PowerShell tip** (common activation snag)

   ```md
   ### Windows PowerShell tip
   If activation is blocked, run once as your user:
   `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`
   ```

6. **“How to reset everything” box** (copy-paste rescue)

   ```md
   ### Full reset (nuclear option)
   docker compose down -v
   docker compose up -d db
   python -m alembic stamp base
   python -m alembic upgrade head
   ```