# MailTrace — Azure-ready Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (psycopg2 build deps, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends         build-essential gcc libpq-dev     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose the port Container Apps/App Service will hit
EXPOSE 8000

# Gunicorn entrypoint (matches Procfile)
COPY docker/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
