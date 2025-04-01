# Script PowerShell para preparar a Layer Python para o Lambda

# Criar a estrutura de diretórios
New-Item -ItemType Directory -Force -Path layers/python/python/lib/python3.11/site-packages

# Instalar dependências no diretório da layer, com versões compatíveis com Python 3.11
pip install -t layers/python/python/lib/python3.11/site-packages `
    pandas==2.0.3 `
    numpy==1.24.3 `
    yfinance==0.2.18 `
    boto3==1.26.0 `
    requests==2.28.0 `
    python-dotenv==0.21.0

# Remover arquivos __pycache__ para reduzir o tamanho
Get-ChildItem -Path layers/python/python/lib/python3.11/site-packages -Recurse -Filter "__pycache__" | 
    ForEach-Object { Remove-Item -Path $_.FullName -Force -Recurse }

# Remover arquivos .pyc e .pyo
Get-ChildItem -Path layers/python/python/lib/python3.11/site-packages -Recurse -Include "*.pyc", "*.pyo" | 
    ForEach-Object { Remove-Item -Path $_.FullName -Force }

Write-Host "Layer preparada com sucesso em layers/python/"
Write-Host "Tamanho da layer:"
$size = (Get-ChildItem layers/python/ -Recurse | Where-Object { !$_.PSIsContainer } | Measure-Object -Property Length -Sum).Sum
Write-Host "$([Math]::Round($size / 1MB, 2)) MB"