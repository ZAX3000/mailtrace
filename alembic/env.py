# alembic/env.py
import os
import sys
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import make_url

config = context.config

# Ensure project root on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass

# Logging
if config.config_file_name and config.get_section("loggers"):
    fileConfig(config.config_file_name)

# Import metadata from your app
from app.extensions import db

# Eager-import model modules so tables register on metadata
try:
    import pkgutil
    from importlib import import_module
    pkg = import_module("app.models")
    for _finder, modname, _ispkg in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        import_module(modname)
except Exception:
    pass

target_metadata = db.metadata

# ---- Require Postgres ----
db_url_str = os.getenv("SQLALCHEMY_DATABASE_URI") or os.getenv("DATABASE_URL")
if not db_url_str:
    raise RuntimeError("DATABASE_URL is required (Postgres).")
url = make_url(db_url_str)

if url.drivername not in ("postgresql", "postgresql+psycopg"):
    raise RuntimeError(
        f"Unsupported DB driver for migrations: '{url.drivername}'. "
        "MailTrace requires Postgres (postgresql or postgresql+psycopg)."
    )

# Force Alembic to use this URL (overrides alembic.ini)
config.set_main_option("sqlalchemy.url", db_url_str)
print("ALEMBIC URL =", config.get_main_option("sqlalchemy.url"))

def run_migrations_offline() -> None:
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
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
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