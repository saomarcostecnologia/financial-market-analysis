# Em src/infrastructure/config/settings.py
# Atualizar para o formato correto com base no seu .env.example
import os
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

class Settings:
    """Configurações da aplicação."""
    
    # AWS
    AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
    S3_DATA_BUCKET = os.getenv("AWS_BUCKET_NAME", "financial-market-data")
    
    # DynamoDB
    DYNAMODB_STOCKS_TABLE = os.getenv("DYNAMODB_STOCKS_TABLE", "financial_stocks")
    DYNAMODB_PRICES_TABLE = os.getenv("DYNAMODB_PRICES_TABLE", "financial_prices")
    
    # API Keys
    ALPHA_VANTAGE_API_KEY_PARAM = os.getenv("ALPHA_VANTAGE_API_KEY_PARAM", "/financial-market/alphavantage/api-key")
    
    # Ambiente
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Projeto
    PROJECT_NAME = os.getenv("PROJECT_NAME", "financial-market-analysis")