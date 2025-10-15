import os
import pytest
from app import create_app
from app.models import db as _db
from alembic import command
from alembic.config import Config as AlembicConfig

TEST_DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://mailtrace:mailtrace@localhost:5433/mailtrace"
)

@pytest.fixture(scope="session")
def app():
    os.environ.setdefault("FLASK_ENV", "testing")
    os.environ.setdefault("DISABLE_AUTH", "1")
    os.environ["DATABASE_URL"] = TEST_DB_URL

    application = create_app()

    # Run Alembic migrations to HEAD against the test DB
    with application.app_context():
        cfg = AlembicConfig("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", TEST_DB_URL)
        command.upgrade(cfg, "head")

    yield application

@pytest.fixture(autouse=True)
def _db_session(app):
    """Wrap each test in a rollback to keep DB clean & fast."""
    conn = _db.engine.connect()
    txn = conn.begin()
    _db.session.bind = conn
    yield
    txn.rollback()
    conn.close()
    _db.session.remove()

@pytest.fixture()
def client(app):
    return app.test_client()

def test_app_boots_and_health_ok(client, _db_session):
    r = client.get("/health")
    assert r.status_code in (200, 204)