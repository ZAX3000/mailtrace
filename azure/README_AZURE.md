# Deploying MailTrace on Azure App Service (Containers)

This repo includes a Dockerfile for running MailTrace on Azure App Service for Linux (Container).

## Why this path
- You already have SQLAlchemy + Alembic. We keep that and use **Cosmos DB for PostgreSQL** (Postgres-compatible).
- The container **entrypoint runs `alembic upgrade head`** before starting gunicorn—so schema is always current.

## Prereqs
- Azure CLI (`az --version`), Docker installed
- Azure subscription
- An Auth0 tenant, a Stripe account
- A Cosmos DB for PostgreSQL cluster & database (or standard Azure Postgres)

## 1) Build and push image to ACR
```bash
az login
az account set --subscription "<SUBSCRIPTION_NAME_OR_ID>"
az group create -n mailtrace-rg -l centralus

# Create ACR
az acr create -g mailtrace-rg -n mailtraceacr123 --sku Basic
az acr login -n mailtraceacr123

# Build & push
docker build -t mailtraceacr123.azurecr.io/mailtrace:latest .
docker push mailtraceacr123.azurecr.io/mailtrace:latest
```

## 2) Create Linux App Service Plan & Web App (Container)
```bash
# App Service Plan
az appservice plan create -g mailtrace-rg -n mailtrace-plan --is-linux --sku P1v3

# Web App
az webapp create -g mailtrace-rg -p mailtrace-plan -n mailtrace-app   --deployment-container-image-name mailtraceacr123.azurecr.io/mailtrace:latest

# Give the Web App a managed identity and grant ACR pull
az webapp identity assign -g mailtrace-rg -n mailtrace-app
APP_PRINCIPAL_ID=$(az webapp show -g mailtrace-rg -n mailtrace-app --query identity.principalId -o tsv)
ACR_ID=$(az acr show -g mailtrace-rg -n mailtraceacr123 --query id -o tsv)
az role assignment create --assignee-object-id $APP_PRINCIPAL_ID --role "AcrPull" --scope $ACR_ID

# Point the web app to ACR and image
az webapp config container set -g mailtrace-rg -n mailtrace-app   --docker-custom-image-name mailtraceacr123.azurecr.io/mailtrace:latest   --docker-registry-server-url https://mailtraceacr123.azurecr.io
```

## 3) Configure App Settings (env vars)
Set values based on `azure/azure.env.sample`. At minimum:
```bash
az webapp config appsettings set -g mailtrace-rg -n mailtrace-app --settings   WEBSITES_PORT=8000   DATABASE_URL="postgresql+psycopg2://USER:PASSWORD@HOST:5432/DB?sslmode=require"   AUTH0_DOMAIN="your-tenant.us.auth0.com"   AUTH0_CLIENT_ID="..."   AUTH0_CLIENT_SECRET="..."   AUTH0_CALLBACK_URL="https://<your-domain>/callback"   AUTH0_LOGOUT_URL="https://<your-domain>/"   STRIPE_SECRET_KEY="sk_live_xxx"   STRIPE_PRICE_BASE="price_xxx"   STRIPE_WEBHOOK_SECRET="whsec_xxx"   AZURE_STORAGE_ACCOUNT="yourstorageacct"   AZURE_STORAGE_KEY="base64accountkey=="   AZURE_STORAGE_CONTAINER="mailtrace-uploads"   MAPBOX_TOKEN=""
```

> Note: `WEBSITES_PORT=8000` tells App Service which exposed container port to route.

## 4) Browse & verify
```bash
az webapp show -g mailtrace-rg -n mailtrace-app --query defaultHostName -o tsv
```
Open `https://<hostname>`. The container entrypoint will run migrations then start gunicorn.

## Updating
- Rebuild/push a new tag, then:
```bash
az webapp config container set -g mailtrace-rg -n mailtrace-app   --docker-custom-image-name mailtraceacr123.azurecr.io/mailtrace:<NEW_TAG>
```
- Or use deployment slots for zero-downtime swaps.

## Troubleshooting
- `az webapp log tail -g mailtrace-rg -n mailtrace-app` for live logs.
- Check container startup events in **Log Stream** and **Container Settings**.
