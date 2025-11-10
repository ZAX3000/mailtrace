# MailTrace API — slim, server-only context
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Optional: you likely don't need build deps if using psycopg[binary]
# If you switch to psycopg[c] or other libs that need compiling, uncomment:
# RUN apt-get update && apt-get install -y --no-install-recommends \
#       build-essential gcc libpq-dev \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first (better layer caching)
COPY requirements.txt requirements.txt
# or: COPY requirements-dev.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy server code only (because build context is server/)
COPY . .

# Non-root user (optional but good practice)
RUN useradd -m appuser
USER appuser

EXPOSE 8000

# Entrypoint (make sure this file is inside server/docker/)
COPY docker/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]