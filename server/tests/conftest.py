# tests/conftest.py
import os
import pytest
from app import create_app

@pytest.fixture(scope="session")
def app():
    os.environ.setdefault("FLASK_ENV", "testing")
    app = create_app()
    app.config.update(TESTING=True)
    with app.app_context():
        yield app

@pytest.fixture()
def client(app):
    return app.test_client()