import os
import sys
from pathlib import Path
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# --- Alembic Config ---
config = context.config

# --- Ensure project root on sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- Load .env so DATABASE_URL is available everywhere ---
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass  # optional

# --- Logging (if configured in alembic.ini) ---
if config.config_file_name and config.get_section("loggers"):
    fileConfig(config.config_file_name)

# --- Import metadata from your app ---
from app.extensions import db  # noqa: E402

# ✅ Ensure all model modules are imported so tables are registered
# If your models are in app/models/*.py this will import them all.
try:
    import pkgutil
    from importlib import import_module

    MODELS_PKG = "app.models"
    pkg = import_module(MODELS_PKG)
    for _finder, modname, _ispkg in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        import_module(modname)
except Exception:
    # If you don't have an app/models package, you can instead do explicit imports here, e.g.:
    # from app.user.models import User  # noqa
    # from app.runs.models import Run   # noqa
    pass

target_metadata = db.metadata

# --- Resolve DB URL (prefer explicit env, fall back to sqlite) ---
db_url = (
    os.getenv("SQLALCHEMY_DATABASE_URI")
    or os.getenv("DATABASE_URL")
    or "sqlite:///instance/local.db"
).strip()

# Make Alembic use this URL (overrides anything in alembic.ini)
config.set_main_option("sqlalchemy.url", db_url)
print("ALEMBIC URL =", config.get_main_option("sqlalchemy.url"))  # debug

def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        render_as_batch=url.startswith("sqlite"),
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
        dialect_name = connection.dialect.name

        # Create tables defined on metadata (no-op if already present)
        try:
            db.metadata.create_all(bind=connection)
        except Exception:
            pass

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            render_as_batch=(dialect_name == "sqlite"),
            version_table_schema="public",
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()