# Instruções de Configuração

Este documento fornece instruções detalhadas para configurar o ambiente de desenvolvimento e execução para o projeto Financial Market Analysis.

## Requisitos do Sistema

### Software Necessário

- **Python**: Versão 3.11 ou superior
- **Node.js**: Versão 14 ou superior (para AWS CDK)
- **Java**: Versão 8 ou superior (para Apache Spark)
- **AWS CLI**: Versão 2 ou superior, configurada com credenciais adequadas
- **Git**: Para controle de versão

### Requisitos de Hardware (Recomendados)

- **CPU**: 4+ cores para processamento de dados em larga escala
- **RAM**: Mínimo 8GB (16GB+ recomendado para Spark)
- **Armazenamento**: 20GB+ de espaço livre

### Conta AWS

- Conta AWS ativa
- Usuário IAM com permissões para:
  - S3
  - DynamoDB
  - Lambda
  - CloudWatch
  - CloudFormation
  - IAM

## Instalação e Configuração

### 1. Clone do Repositório

```bash
git clone https://github.com/saomarcostecnologia/financial-market-analysis.git
cd financial-market-analysis
```

### 2. Configuração do Ambiente Python

#### Criação do Ambiente Virtual

```bash
# Linux/MacOS
python -m venv venv
source venv/bin/activate

# Windows (PowerShell)
python -m venv venv
.\venv\Scripts\Activate.ps1

# Windows (Command Prompt)
python -m venv venv
.\venv\Scripts\activate.bat
```

#### Instalação das Dependências

```bash
pip install -r requirements.txt
```

### 3. Configuração do AWS CDK

```bash
cd infrastructure/cdk
npm install
```

### 4. Configuração do Apache Spark (Opcional)

Se você planeja usar o Apache Spark para processamento de dados:

#### Linux/MacOS

```bash
# Configurar variável JAVA_HOME
export JAVA_HOME=/path/to/your/java
```

#### Windows

```powershell
# Configurar variável JAVA_HOME no PowerShell
$env:JAVA_HOME = "C:\path\to\your\java"

# Ou no CMD
set JAVA_HOME=C:\path\to\your\java
```

### 5. Configuração das Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto baseado no exemplo fornecido:

```bash
cp .env.example .env
```

Edite o arquivo `.env` com suas configurações:

```
# AWS Credentials
AWS_ACCESS_KEY_ID=SUA_CHAVE_AQUI
AWS_SECRET_ACCESS_KEY=SUA_CHAVE_SECRETA_AQUI
AWS_REGION=sa-east-1
AWS_BUCKET_NAME=seu-bucket-datalake

# Environment
ENVIRONMENT=development
LOG_LEVEL=INFO

# Project Settings
PROJECT_NAME=financial-market-analysis

# API Keys (opcional)
ALPHA_VANTAGE_API_KEY=sua_chave_alpha_vantage
```

## Configuração das APIs Financeiras

### Yahoo Finance

O Yahoo Finance não requer chave de API para uso básico, mas tem limitações de taxa de requisição.

### Alpha Vantage (Opcional)

1. Obtenha uma chave de API gratuita em [Alpha Vantage](https://www.alphavantage.co/support/#api-key)
2. Adicione sua chave ao arquivo `.env` ou ao AWS Parameter Store

## Configuração do Armazenamento

### Opção 1: Usar S3 e DynamoDB Existentes

Se você já possui buckets S3 e tabelas DynamoDB:

1. Atualize o arquivo `.env` com os nomes dos recursos
2. Certifique-se de que seu usuário IAM tem permissões adequadas

### Opção 2: Provisionar Nova Infraestrutura

Para criar nova infraestrutura:

```bash
cd infrastructure/cdk
npx cdk bootstrap aws://ACCOUNT-NUMBER/REGION
npx cdk deploy --context environment=dev
```

## Testando a Configuração

### Verificar Conectividade AWS

```bash
python -c "import boto3; s3 = boto3.client('s3'); print(s3.list_buckets())"
```

### Verificar Extração de Dados

```bash
python -c "import yfinance as yf; data = yf.download('AAPL', period='5d'); print(data.head())"
```

### Executar Teste End-to-End

```bash
python tests/e2e/pipeline/test_full_pipeline.py --ticker AAPL --days 7 --steps bronze
```

## Solução de Problemas

### Problemas Comuns

#### Erro de Autenticação AWS

```
botocore.exceptions.ClientError: An error occurred (AccessDenied)
```

**Solução**: Verifique suas credenciais AWS no arquivo `.env` ou nas variáveis de ambiente.

#### Problemas com Apache Spark

```
Java gateway process exited before sending its port number
```

**Solução**: Verifique se o Java está instalado e configurado corretamente. Configure a variável JAVA_HOME.

#### Erros de Importação de Módulos

```
ModuleNotFoundError: No module named 'src'
```

**Solução**: Certifique-se de estar na raiz do projeto ou adicione a raiz ao PYTHONPATH:

```bash
# Linux/MacOS
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Windows
set PYTHONPATH=%PYTHONPATH%;%cd%
```

## Próximos Passos

Após concluir a configuração:

1. Consulte [docs/PIPELINE.md](PIPELINE.md) para entender o pipeline de dados
2. Consulte [docs/AWS_DEPLOYMENT.md](AWS_DEPLOYMENT.md) para implantação em produção
3. Experimente executar o pipeline completo com dados reais