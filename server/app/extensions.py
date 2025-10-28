# app/extensions.py
from __future__ import annotations

import os
import stripe
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from typing import NoReturn

db = SQLAlchemy()
migrate = Migrate()

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")

# --- Storage intentionally removed ---
class _StorageDisabled:
    def __getattr__(self, _name: str) -> NoReturn:
        raise RuntimeError(
            "Storage is disabled in this repo. "
            "Reintroduce a storage service later if needed."
        )

def storage() -> _StorageDisabled:  # helper for any lingering imports
    return _StorageDisabled()