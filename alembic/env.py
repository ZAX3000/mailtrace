# alembic/env.py
from __future__ import annotations

import os
import sys
import logging
from pathlib import Path
from logging.config import fileConfig
from typing import Any, Dict

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

# -------------------------------------------------------------------
# Path setup (must happen before importing project modules)
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env so Alembic has DATABASE_URL/SQLALCHEMY_DATABASE_URI available
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")  # no crash if missing
except Exception as e:  # log and continue
    logging.getLogger(__name__).debug("dotenv not loaded: %s", e)

# Now safe to import app modules
from app.extensions import db  # noqa: E402

# Alembic Config object
config = context.config

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
if config.config_file_name:
    fileConfig(config.config_file_name)
logger = logging.getLogger("alembic.env")

# -------------------------------------------------------------------
# Eager-import models so metadata is populated
# -------------------------------------------------------------------
try:
    import pkgutil
    from importlib import import_module

    pkg = import_module("app.models")
    for _finder, modname, _ispkg in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        import_module(modname)
except Exception as e:
    logger.debug("Model import walk failed (continuing): %s", e)

target_metadata = db.metadata

# -------------------------------------------------------------------
# Require Postgres and set URL
# -------------------------------------------------------------------
db_url_str: str | None = os.getenv("SQLALCHEMY_DATABASE_URI") or os.getenv("DATABASE_URL")
if not db_url_str:
    raise RuntimeError("DATABASE_URL (or SQLALCHEMY_DATABASE_URI) is required and must point to Postgres.")

url = make_url(db_url_str)
if url.drivername not in ("postgresql", "postgresql+psycopg"):
    raise RuntimeError(
        f"Unsupported DB driver for migrations: '{url.drivername}'. "
        "MailTrace requires Postgres (postgresql or postgresql+psycopg)."
    )

# Force Alembic to use this URL (overrides alembic.ini)
config.set_main_option("sqlalchemy.url", db_url_str)
logger.info("Alembic using URL: %s", config.get_main_option("sqlalchemy.url"))

# -------------------------------------------------------------------
# Offline / Online runners
# -------------------------------------------------------------------
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (no DB API connection)."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        version_table_schema="public",
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (uses DB API connection)."""
    ini_section: Dict[str, Any] = dict(config.get_section(config.config_ini_section) or {})
    connectable = engine_from_config(
        ini_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            version_table_schema="public",
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()