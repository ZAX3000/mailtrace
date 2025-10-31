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

1. **Start Docker Desktop** → wait for **Engine running**.
2. In the repo root:

   ```bash
   docker compose up -d db
   ```
3. Activate venv & env, **run Alembic** from `/server`, start Flask:

   ```bash
   # Windows Git Bash/MSYS2
   source .venv/Scripts/activate
   cd server
   # DATABASE_URL loaded from ../.env by env.py (or set it inline)
   alembic upgrade head
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

Start it (make sure the Docker app is running first!):

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

*(keep your section as-is, but add this tip at the end)*

> **Tip:** Alembic reads `DATABASE_URL` via `server/migrations/env.py`. We load `../.env` (repo root). You can also export/set `DATABASE_URL` manually.

---

## 6) Apply database migrations (**Alembic**)

> We now use **Alembic CLI directly** (not `flask db …`).

```bash
# From repo root
docker compose up -d db

# In a new shell
source .venv/Scripts/activate        # or source .venv/bin/activate (macOS/Linux)
cd server
alembic upgrade head
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
# Still inside /server
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
cd server
alembic revision --autogenerate -m "describe change"
alembic upgrade head
```

### See current DB revision / heads

```bash
cd server
alembic current
alembic heads
```

### Reset the local DB completely (fresh containers + schema)

```bash
docker compose down -v
docker compose up -d db
cd server
alembic upgrade head
```

### If your DB was manually changed and you need to realign Alembic

* **Mark empty DB as baseline without running DDL**:

  ```bash
  cd server
  alembic stamp head
  ```
* **Mark DB as empty (dangerous; dev only)**:

  ```bash
  cd server
  alembic stamp base
  ```

---

## 10) Troubleshooting (Alembic-specific)

* **`Target database is not up to date.`**
  You have pending migrations on disk. Run:

  ```bash
  cd server && alembic upgrade head
  ```

* **`relation "public.alembic_version" does not exist` during `upgrade`**
  Ensure `server/migrations/env.py` does **not** set `version_table_schema`, and your migration files don’t touch `alembic_version`. We ignore it via `include_object` in `env.py`.

* **Downgrade errors like `index "idx_…" does not exist`**
  This happens if you previously `stamp`ed without running `upgrade`. Easiest fix:

  ```bash
  cd server
  alembic stamp base
  alembic upgrade head
  ```

* **Auth failures connecting to Postgres**
  `.env` must match docker creds:

  ```
  DATABASE_URL=postgresql+psycopg://mailtrace:devpass@localhost:5433/mailtrace
  ```

  If changed, `docker compose down -v && docker compose up -d db`.

---

## 11) What’s in this setup (and why)

*(keep as-is, but add)*

* **Alembic** is the single source of truth for schema changes (via `server/migrations`).

---

### Appendix A — Project layout (updated)

```
├─ server/
│  ├─ app/                   # create_app(), routes, services, repositories, models, templates, static
│  └─ migrations/            # Alembic
│     ├─ env.py              # loads ../.env, imports app models, ignores alembic_version
│     ├─ alembic.ini
│     └─ versions/           # migration files
├─ docker-compose.yml        # postgres:16 on host 5433
├─ requirements.txt
├─ .env.example
└─ README.md
```

### Appendix B — Quick demo (no login)

```bash
docker compose up -d db
cd server
alembic upgrade head
cd ..
# .env: DISABLE_AUTH=1
cd server && python -m flask --app app:create_app run --debug
```

### Appendix C — Full reset (nuclear option)

```bash
docker compose down -v
docker compose up -d db
cd server && alembic upgrade head
```

---

## Optional: tiny helper scripts

**`start-dev.sh`**

```bash
#!/usr/bin/env bash
set -e
docker compose up -d db
source .venv/bin/activate
cd server
alembic upgrade head
python -m flask --app app:create_app run --debug -h 127.0.0.1 -p 5000
```

**`start-dev.cmd` (Windows)**

```bat
@echo off
docker compose up -d db
call .venv\Scripts\activate
cd server
alembic upgrade head
python -m flask --app app:create_app run --debug -h 127.0.0.1 -p 5000
```