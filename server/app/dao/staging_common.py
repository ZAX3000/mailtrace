# app/dao/staging_common.py
from sqlalchemy.engine import Engine

def assert_postgres(engine: Engine) -> None:
    name = engine.url.get_backend_name()
    if not name.startswith("postgresql"):
        raise RuntimeError(f"Expected Postgres engine, got {name!r}")