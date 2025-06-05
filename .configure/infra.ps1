$ErrorActionPreference = 'Stop'

# Caminho do .env
$envFilePath = "src\backend\.env"

# Função para extrair variável do .env
function Get-EnvValue($name) {
    return (Get-Content $envFilePath | Where-Object { $_ -match "^$name\s*=" }) -replace '.*=\s*"?([^"]+)"?', '$1'
}

# Variáveis do .env
$sub = Get-EnvValue 'SUBSCRIPTION_ID'
$location = Get-EnvValue 'LOCATION'
$rgName = Get-EnvValue 'RESOURCE_GROUP'
$vnetName = Get-EnvValue 'VNET_NAME'
$subnetName = Get-EnvValue 'MANAGEMENT_SUBNET_NAME'

# Definir subscription atual
Write-Host "✅ Subscription: $sub"
az account set --subscription $sub

# Lista de contas de IA e seus parâmetros de restauração
$accounts = @(
  @{ Name = Get-EnvValue 'AZURE_OPENAI_ACCOUNT_NAME';    Param = 'openAiRestore' }
)

# Detecta e configura restore
$restoreParams = @{}

foreach ($acct in $accounts) {
  $name = $acct.Name
  $param = $acct.Param

  $deleted = az cognitiveservices account list-deleted `
    --output json | ConvertFrom-Json

  $match = $deleted | Where-Object { $_.name -eq $name }

  if ($match) {
      Write-Host "🔁 Soft-deleted: $name. Adding '$param=true'"
      $restoreParams[$param] = $true
  } else {
      $restoreParams[$param] = $false
  }
}

Write-Host "🔧 Parâmetros de restauração: $($restoreParams | Out-String)"

function Set-CognitiveServicesEndpoint {
    Write-Host "🔧 Verificando endpoint Microsoft.CognitiveServices..."
    $endpoints = az network vnet subnet show `
        --resource-group $rgName `
        --vnet-name $vnetName `
        --name $subnetName `
        --query "serviceEndpoints[].service" -o tsv

    if (-not ($endpoints -contains 'Microsoft.CognitiveServices')) {
        Write-Host "🔁 Adicionando endpoint Microsoft.CognitiveServices..."
        az network vnet subnet update `
            --resource-group $rgName `
            --vnet-name $vnetName `
            --name $subnetName `
            --service-endpoints Microsoft.CognitiveServices | Out-Null
    }
}

# Inicia deploy principal
function Deploy-MainTemplate {
    # core params
    $paramArgs = @(
      "rgName=$rgName"
      "location=$location"
    )
    # add any restore flags
    foreach ($kvp in $restoreParams.GetEnumerator()) {
      $paramArgs += "$($kvp.Key)=$($kvp.Value)"
    }

    # build the full CLI argument list
    $cliArgs = @(
      "deployment" 
      "sub" 
      "create"
      "--location" 
      $location
      "--template-file" 
      "$PSScriptRoot\..\infra\main.bicep"
      "--parameters"
    ) + $paramArgs + @(
      "--verbose"
      "--debug"
    )

    Write-Host "🔧 Executando deploy: az $($cliArgs -join ' ')"

    # invoke Azure CLI with splatted array
    $output = az @cliArgs

    return $output
}

try {
    Write-Host "🚀 Iniciando deploy da infraestrutura principal..."
    Set-CognitiveServicesEndpoint
    $result = Deploy-MainTemplate
    Write-Host "🔧 Resultado do deploy: $($result | Out-String)"
    if (($null -ne $result | Out-String | Select-String 'ERROR')) {
        throw "❌ Deploy returned ERROR in output"
    }
    Write-Host "✅ Deploy finalizado com sucesso!"
} catch {
    Write-Host "❌ Falha no deploy: $_"
    Write-Host "📋 Listando operações com erro..."
    az deployment operation sub list `
        --name main `
        --query "[?properties.provisioningState=='Failed']" `
        --output table
    exit 1
} finally {
    Write-Host "⚙️ Script finalizado em $(Get-Date -Format o)"
}
