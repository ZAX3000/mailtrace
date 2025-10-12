param(
  [string]$ResourceGroup = "mailtrace-rg",
  [string]$Location = "centralus",
  [string]$AcrName = "mailtraceacr123",
  [string]$PlanName = "mailtrace-plan",
  [string]$AppName = "mailtrace-app",
  [string]$ImageTag = "latest"
)

az login
az account show 1>$null 2>$null

# Resource group
az group create -n $ResourceGroup -l $Location

# ACR
az acr create -g $ResourceGroup -n $AcrName --sku Basic
az acr login -n $AcrName

# Build & push
docker build -t "$AcrName.azurecr.io/mailtrace:$ImageTag" .
docker push "$AcrName.azurecr.io/mailtrace:$ImageTag"

# App Service Plan (Linux)
az appservice plan create -g $ResourceGroup -n $PlanName --is-linux --sku P1v3

# Web App (Container)
az webapp create -g $ResourceGroup -p $PlanName -n $AppName `
  --deployment-container-image-name "$AcrName.azurecr.io/mailtrace:$ImageTag"

# Managed identity + ACR pull
az webapp identity assign -g $ResourceGroup -n $AppName
$APP_PRINCIPAL_ID = az webapp show -g $ResourceGroup -n $AppName --query identity.principalId -o tsv
$ACR_ID = az acr show -g $ResourceGroup -n $AcrName --query id -o tsv
az role assignment create --assignee-object-id $APP_PRINCIPAL_ID --role "AcrPull" --scope $ACR_ID

# Container registry linkage
az webapp config container set -g $ResourceGroup -n $AppName `
  --docker-custom-image-name "$AcrName.azurecr.io/mailtrace:$ImageTag" `
  --docker-registry-server-url "https://$AcrName.azurecr.io"

# Env vars from azure\azure.env.sample (user should edit with real values first)
$envFile = "azure\azure.env.sample"
$envVars = Get-Content $envFile | Where-Object {$_ -and -not $_.StartsWith("#")}
$settings = @("WEBSITES_PORT=8000")
foreach ($line in $envVars) {
  $kv = $line.Split("=",2)
  if ($kv.Count -eq 2) {
    $key = $kv[0].Trim()
    $val = $kv[1].Trim()
    if ($val -ne "") { $settings += "$key=$val" }
  }
}
az webapp config appsettings set -g $ResourceGroup -n $AppName --settings $settings

# Show URL
$HOST = az webapp show -g $ResourceGroup -n $AppName --query defaultHostName -o tsv
Write-Host "Public URL: https://$HOST"
