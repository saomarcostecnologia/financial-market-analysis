# src/infrastructure/config/data_lake_settings.py
from datetime import datetime

class DataLakeSettings:
    """Configurações para as camadas do Data Lake."""
    
    BRONZE_PREFIX = "bronze/"  # Dados brutos
    SILVER_PREFIX = "silver/"  # Dados processados
    GOLD_PREFIX = "gold/"     # Dados analíticos
    
    @staticmethod
    def get_bronze_path(ticker, data_type, date):
        """Obter caminho para dados na camada bronze."""
        return f"{DataLakeSettings.BRONZE_PREFIX}stocks/{ticker}/{data_type}/year={date.year}/month={date.month:02d}/day={date.day:02d}/"
    
    @staticmethod
    def get_silver_path(ticker, data_type, date):
        """Obter caminho para dados na camada silver."""
        return f"{DataLakeSettings.SILVER_PREFIX}stocks/{ticker}/{data_type}/year={date.year}/month={date.month:02d}/"
    
    @staticmethod
    def get_gold_path(ticker, data_type):
        """Obter caminho para dados na camada gold."""
        return f"{DataLakeSettings.GOLD_PREFIX}stocks/{ticker}/{data_type}/"