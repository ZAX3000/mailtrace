from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import stripe, os
try:
    import boto3
except Exception:
    boto3 = None
from azure.storage.blob import BlobServiceClient

db = SQLAlchemy()
migrate = Migrate()

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")


def s3():
    # Backward-compatible storage accessor.
    backend = os.environ.get("STORAGE_BACKEND", os.environ.get("STORAGE_PROVIDER", "azure")).lower()
    if backend.startswith("azure"):
        conn = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
        container_env = os.environ.get("AZURE_STORAGE_CONTAINER") or os.environ.get("AWS_S3_BUCKET") or ""
        if not conn or not container_env:
            raise RuntimeError("Azure storage requires AZURE_STORAGE_CONNECTION_STRING and AZURE_STORAGE_CONTAINER (or AWS_S3_BUCKET as alias).")
        svc = BlobServiceClient.from_connection_string(conn)
        class AzureS3Shim:
            def __init__(self, svc): self.svc = svc
            def upload_fileobj(self, fileobj, container, key):
                container = container_env or container
                self.svc.get_container_client(container).upload_blob(name=key, data=fileobj, overwrite=True)
            def upload_file(self, filename, container, key):
                container = container_env or container
                with open(filename, "rb") as f:
                    self.svc.get_container_client(container).upload_blob(name=key, data=f, overwrite=True)
        return AzureS3Shim(svc)
    # default to AWS S3 client if available
    if boto3 is None:
        raise RuntimeError("boto3 not available and STORAGE_BACKEND is not azure.")
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
