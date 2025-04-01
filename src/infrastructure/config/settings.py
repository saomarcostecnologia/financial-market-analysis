# src/infrastructure/config/settings.py
import os
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

class Settings:
    """Configurações da aplicação."""
    
    # AWS
    AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
    S3_DATA_BUCKET = os.getenv("AWS_BUCKET_NAME", "financial-market-data-dev-779618327552")
    
    # DynamoDB
    DYNAMODB_STOCKS_TABLE = os.getenv("DYNAMODB_STOCKS_TABLE", "financial_stocks")
    DYNAMODB_PRICES_TABLE = os.getenv("DYNAMODB_PRICES_TABLE", "financial_prices")
    
    # API Keys
    ALPHA_VANTAGE_API_KEY_PARAM = os.getenv("ALPHA_VANTAGE_API_KEY_PARAM", "/financial-market/alphavantage/api-key")
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
    
    # Ambiente
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Projeto
    PROJECT_NAME = os.getenv("PROJECT_NAME", "financial-market-analysis")