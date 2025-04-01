# Script para carregar variáveis do .env e executar o comando CDK

# Carrega as variáveis do arquivo .env
$envContent = Get-Content .env -ErrorAction SilentlyContinue
if (-not $envContent) {
    $envContent = Get-Content ../.env -ErrorAction SilentlyContinue
}

if ($envContent) {
    Write-Host "Carregando variáveis de ambiente do arquivo .env..." -ForegroundColor Cyan
    foreach ($line in $envContent) {
        # Ignora linhas de comentário e linhas vazias
        if ($line -match '^\s*#' -or $line -match '^\s*$') {
            continue
        }

        # Extrai nome e valor da variável
        if ($line -match '^\s*([^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            
            # Remove aspas se existirem
            $value = $value -replace '^"(.*)"$', '$1'
            $value = $value -replace "^'(.*)'$", '$1'
            
            # Define a variável de ambiente
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
            Write-Host "Definida variável: $name" -ForegroundColor DarkGray
        }
    }
    
    # Agora que as variáveis estão carregadas, execute o comando CDK
    Write-Host "Executando comando CDK..." -ForegroundColor Green
    
    # Verifica se estamos no diretório cdk
    $currentDir = Split-Path -Leaf (Get-Location)
    if ($currentDir -ne "cdk") {
        cd infrastructure/cdk
        Write-Host "Navegando para o diretório infrastructure/cdk" -ForegroundColor Yellow
    }
    
    # Exibe as variáveis AWS que serão usadas
    Write-Host "Usando as seguintes credenciais AWS:" -ForegroundColor Cyan
    Write-Host "AWS_REGION: $env:AWS_REGION" -ForegroundColor Cyan
    Write-Host "AWS_ACCESS_KEY_ID: $($env:AWS_ACCESS_KEY_ID.Substring(0, 4))..." -ForegroundColor Cyan
    
    # Executa o comando CDK destroy
    npx cdk destroy --context environment=$env:ENVIRONMENT
} else {
    Write-Host "Arquivo .env não encontrado. Verifique se o arquivo existe na raiz do projeto." -ForegroundColor Red
}