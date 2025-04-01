# Script PowerShell para preparar a Layer Python para o Lambda

# Criar a estrutura de diretórios
New-Item -ItemType Directory -Force -Path layers/python/python/lib/python3.9/site-packages

# Instalar dependências no diretório da layer
pip install -t layers/python/python/lib/python3.9/site-packages `
    pandas==2.0.0 `
    numpy==1.20.0 `
    yfinance==0.2.18 `
    boto3==1.26.0 `
    requests==2.28.0 `
    python-dotenv==0.21.0

# Remover arquivos __pycache__ para reduzir o tamanho
Get-ChildItem -Path layers/python/python/lib/python3.9/site-packages -Recurse -Filter "__pycache__" | 
    ForEach-Object { Remove-Item -Path $_.FullName -Force -Recurse }

# Remover arquivos .pyc e .pyo
Get-ChildItem -Path layers/python/python/lib/python3.9/site-packages -Recurse -Include "*.pyc", "*.pyo" | 
    ForEach-Object { Remove-Item -Path $_.FullName -Force }

Write-Host "Layer preparada com sucesso em layers/python/"
Write-Host "Tamanho da layer:"
Get-ChildItem layers/python/ -Recurse | Measure-Object -Property Length -Sum