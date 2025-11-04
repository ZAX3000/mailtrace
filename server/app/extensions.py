# app/extensions.py
from __future__ import annotations

import os
import stripe
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

db = SQLAlchemy()
migrate = Migrate()

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")

