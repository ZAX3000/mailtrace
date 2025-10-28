import os
class Config:
    DISABLE_AUTH = os.environ.get('DISABLE_AUTH', '0') in ('1','true','True')
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev")
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL", "sqlite:///local.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Storage backend (azure|local)
    STORAGE_BACKEND = os.environ.get("STORAGE_BACKEND", "local")
    # Azure Blob Storage
    AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
    AZURE_STORAGE_CONTAINER = os.environ.get("AZURE_STORAGE_CONTAINER", "")

    # Auth0
    AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN", "")
    AUTH0_CLIENT_ID = os.environ.get("AUTH0_CLIENT_ID", "")
    AUTH0_CLIENT_SECRET = os.environ.get("AUTH0_CLIENT_SECRET", "")
    AUTH0_CALLBACK_URL = os.environ.get("AUTH0_CALLBACK_URL", "")
    AUTH0_LOGOUT_URL = os.environ.get("AUTH0_LOGOUT_URL", "")

    # Stripe
    STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
    STRIPE_PRICE_BASE = os.environ.get("STRIPE_PRICE_BASE", "")
    STRIPE_PRICE_METERED = os.environ.get("STRIPE_PRICE_METERED", "")
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
    STRIPE_SUCCESS_URL = os.environ.get("STRIPE_SUCCESS_URL", "")
    STRIPE_CANCEL_URL = os.environ.get("STRIPE_CANCEL_URL", "")

    MAPBOX_TOKEN = os.environ.get("MAPBOX_TOKEN", "")
