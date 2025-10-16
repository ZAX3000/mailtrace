# app/extensions.py
from __future__ import annotations

import os
import stripe
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from typing import NoReturn

db = SQLAlchemy()
migrate = Migrate()

# Stripe can stay; if unused it’s harmless. Remove if you prefer.
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")

# --- Storage intentionally removed ---
# If any legacy code calls storage, fail fast with a clear error.
class _StorageDisabled:
    def __getattr__(self, _name: str) -> NoReturn:
        raise RuntimeError(
            "Storage is disabled in this repo. "
            "We’ve removed Azure/S3 legacy. Reintroduce a storage service later if needed."
        )

def storage() -> _StorageDisabled:  # helper for any lingering imports
    return _StorageDisabled()

# Legacy alias (in case something still imports s3()); remove once references are gone.
def s3() -> _StorageDisabled:
    return storage()