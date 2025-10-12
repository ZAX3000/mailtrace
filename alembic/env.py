import os
import sys
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config

# Ensure project root import
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Logging if configured
if config.config_file_name and config.get_section("loggers"):
    fileConfig(config.config_file_name)

# Import metadata
from app.extensions import db
target_metadata = db.metadata

# Resolve DB URL with SQLite fallback
_db_url = os.getenv("DATABASE_URL", "").strip() or "sqlite:///instance/local.db"
config.set_main_option("sqlalchemy.url", _db_url)

def run_migrations_offline():
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        render_as_batch=url.startswith("sqlite"),
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        dialect_name = connection.dialect.name
        # Create tables from SQLAlchemy models (idempotent)
        try:
            db.metadata.create_all(bind=connection)
        except Exception:
            pass
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=(dialect_name == "sqlite"),
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
