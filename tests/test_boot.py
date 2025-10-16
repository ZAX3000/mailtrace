# tests/test_boot.py
import os
import pytest
from app import create_app
from app.models import db as _db
from alembic import command
from alembic.config import Config as AlembicConfig

TEST_DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://mailtrace:devpass@localhost:5433/mailtrace",
)

@pytest.fixture(scope="session")
def app():
    TEST_DB_URL = os.environ.get(
        "DATABASE_URL",
        "postgresql+psycopg://mailtrace:devpass@localhost:5433/mailtrace",
    )

    os.environ["FLASK_ENV"] = "testing"
    os.environ["DISABLE_AUTH"] = "1"
    os.environ["DATABASE_URL"] = TEST_DB_URL
    os.environ["SQLALCHEMY_DATABASE_URI"] = TEST_DB_URL

    application = create_app()

    try:
        print("APP SQLALCHEMY_DATABASE_URI =", application.config.get("SQLALCHEMY_DATABASE_URI"))
    except Exception:
        pass

    with application.app_context():
        cfg = AlembicConfig("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", TEST_DB_URL)
        command.upgrade(cfg, "head")

    return application

@pytest.fixture(autouse=True)
def _db_session(app):
    """Wrap each test in a transaction and roll it back; run inside app context."""
    with app.app_context():
        conn = _db.engine.connect()
        txn = conn.begin()
        _db.session.bind = conn
        try:
            yield
        finally:
            txn.rollback()
            conn.close()
            _db.session.remove()

@pytest.fixture()
def client(app):
    return app.test_client()

def test_app_boots_and_health_ok(client, _db_session):
    r = client.get("/healthz")
    assert r.status_code in (200, 204)