# server/migrations/env.py
from __future__ import annotations

import logging
import os
import sys
from logging.config import fileConfig
from pathlib import Path
from typing import Any, Dict

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

# ------------------------------------------------------------
# Alembic config (must be set before using fileConfig / options)
# ------------------------------------------------------------
config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
logger = logging.getLogger("alembic.env")

# ------------------------------------------------------------
# Ensure /server is importable so `import app` works
# env.py is in server/migrations/, so parents[1] == /server
# ------------------------------------------------------------
SERVER_ROOT = Path(__file__).resolve().parents[1]
if str(SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVER_ROOT))

# Optionally load .env from repo root (one level above /server)
REPO_ROOT = SERVER_ROOT.parent
try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except Exception as e:
    logger.debug("dotenv not loaded: %s", e)

# ------------------------------------------------------------
# Import metadata from your app
# ------------------------------------------------------------
from app.extensions import db  # noqa: E402

# Eager-import models so autogenerate sees all tables
try:
    import pkgutil
    from importlib import import_module

    pkg = import_module("app.models")
    for _finder, modname, _ispkg in pkgutil.walk_packages(
        pkg.__path__, pkg.__name__ + "."
    ):
        import_module(modname)
except Exception as e:
    logger.debug("Model import walk failed (continuing): %s", e)

target_metadata = db.metadata

# ------------------------------------------------------------
# Database URL (Postgres-only guard)
# ------------------------------------------------------------
db_url_str: str | None = os.getenv("DATABASE_URL") or os.getenv("SQLALCHEMY_DATABASE_URI")
if not db_url_str:
    raise RuntimeError("Set DATABASE_URL (or SQLALCHEMY_DATABASE_URI) for Alembic.")
driver = make_url(db_url_str).drivername
if driver not in ("postgresql", "postgresql+psycopg"):
    raise RuntimeError(
        f"Unsupported driver: {driver}. Use Postgres (postgresql or postgresql+psycopg)."
    )

# Force Alembic to use this URL (overrides alembic.ini)
config.set_main_option("sqlalchemy.url", db_url_str)
logger.info("Alembic using URL: %s", config.get_main_option("sqlalchemy.url"))

# ------------------------------------------------------------
# Autogenerate filter: never diff Alembic's own version table
# ------------------------------------------------------------
def include_object(obj, name, type_, reflected, compare_to):
    if type_ == "table" and name == "alembic_version":
        return False
    return True

# ------------------------------------------------------------
# Offline / Online runners
# ------------------------------------------------------------
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        include_object=include_object,  # <-- important
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    ini_section: Dict[str, Any] = dict(config.get_section(config.config_ini_section) or {})
    connectable = engine_from_config(ini_section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_object=include_object,  # <-- important
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()