# Guia de Configuração do Projeto

Este documento fornece instruções detalhadas para configurar o ambiente de desenvolvimento para o projeto de Engenharia de Dados para Mercado Financeiro.

## Pré-requisitos

### Requisitos de Software

- **Python 3.11+**
- **AWS CLI** instalado e configurado
- **Node.js 14+** (para AWS CDK)
- **Java 8+** (para Apache Spark)
- **Hadoop** (opcional, para execução local do Spark em Windows)

### Requisitos de Conta

- Conta AWS com permissões para os seguintes serviços:
  - S3
  - DynamoDB
  - Lambda
  - CloudWatch
  - CloudFormation
  - IAM

## Instalação

### 1. Clone o Repositório

```bash
git clone https://github.com/seu-usuario/financial-market-analysis.git
cd financial-market-analysis
```

### 2. Configuração do Ambiente Python

#### Criação do Ambiente Virtual

```bash
# Linux/macOS
python -m venv venv
source venv/bin/activate

# Windows
python -m venv venv
venv\Scripts\activate
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

## Configuração

### 1. Arquivo de Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com base no `.env.example`:

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
```

### 2. Configuração do Spark (Opcional para execução local)

#### Linux/macOS

```bash
# Instale o PySpark via pip
pip install pyspark
```

#### Windows

Para Windows, é necessário configurar o Hadoop:

1. Baixe o Hadoop a partir de https://hadoop.apache.org/releases.html
2. Extraia para um diretório (ex: `C:\hadoop`)
3. Defina a variável de ambiente `HADOOP_HOME` para apontar para este diretório
4. Adicione `%HADOOP_HOME%\bin` ao PATH
5. Copie os binários do winutils para `%HADOOP_HOME%\bin`

### 3. Configuração de APIs Externas

#### Alpha Vantage API

1. Registre-se em https://www.alphavantage.co/support/#api-key
2. Adicione a API key ao arquivo `.env`:

```
ALPHA_VANTAGE_API_KEY=sua_api_key_aqui
```

Ou, para maior segurança, armazene no AWS Parameter Store:

```bash
aws ssm put-parameter --name "/financial-market/alphavantage/api-key" --value "sua_api_key_aqui" --type SecureString
```

## Configuração de Camadas do Lambda

Para preparar as camadas do Lambda:

### Linux/macOS

```bash
bash scripts/prepare_lambda_layer.sh
```

### Windows

```powershell
.\scripts\prepare_lambda_layer.ps1
```

## Verificar Instalação

Para verificar se tudo foi instalado corretamente:

```bash
# Teste do ambiente Python
python -c "import pandas; import numpy; import boto3; print('Ambiente básico OK')"

# Se você for usar Spark
python -c "import findspark; findspark.init(); import pyspark; print('Spark OK')"

# Teste do AWS CDK
cd infrastructure/cdk
npx cdk --version
```

## Execução Local

Para testar o pipeline localmente:

```bash
# Teste ETL básico
python scripts/test_stock_etl.py --ticker AAPL --days 30

# Teste Lakehouse
python scripts/test_lakehouse.py --ticker AAPL --days 30

# Pipeline completo (Bronze, Silver, Gold)
python tests/e2e/pipeline/test_full_pipeline.py --ticker AAPL --days 30
```

## Solução de Problemas

### Problemas com PySpark

1. Verifique se o Java está instalado corretamente:
   ```bash
   java -version
   ```

2. Para Windows, certifique-se de que o HADOOP_HOME está configurado:
   ```bash
   echo %HADOOP_HOME%
   ```

3. Se o Spark falhar, o sistema automaticamente usará o Pandas para processamento.

### Problemas com AWS

1. Verifique suas credenciais:
   ```bash
   aws sts get-caller-identity
   ```

2. Verifique se o bucket S3 existe:
   ```bash
   aws s3 ls s3://seu-bucket-datalake
   ```

### Problemas com AWS CDK

1. Reinstale as dependências:
   ```bash
   cd infrastructure/cdk
   rm -rf node_modules
   npm install
   ```

2. Verifique a versão do CDK:
   ```bash
   npx cdk --version
   ```