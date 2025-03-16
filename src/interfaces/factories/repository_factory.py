# src/interfaces/factories/repository_factory.py
from src.infrastructure.repositories.s3_repository import S3StockRepository, S3MarketDataRepository
from src.infrastructure.repositories.dynamo_repository import DynamoDBStockRepository
from src.infrastructure.adapters.yahoo_finance_adapter import YahooFinanceAdapter
from src.infrastructure.adapters.alpha_vantage_adapter import AlphaVantageAdapter
from src.infrastructure.services.aws_observability_service import AWSObservabilityService
from src.infrastructure.services.simple_data_masking_service import SimpleDataMaskingService
from src.infrastructure.services.pandas_data_processing_service import PandasDataProcessingService
from src.infrastructure.services.spark_data_processing_service import SparkDataProcessingService
from src.infrastructure.config.settings import Settings
import logging

logger = logging.getLogger(__name__)

class RepositoryFactory:
    """Factory for creating repository and service instances."""
    
    @staticmethod
    def create_settings():
        """Create Settings instance."""
        return Settings()
    
    @staticmethod
    def create_stock_repository(repo_type='dynamo', settings=None):
        """Create a stock repository instance."""
        settings = settings or RepositoryFactory.create_settings()
        
        if repo_type == 's3':
            return S3StockRepository(settings)
        else:
            return DynamoDBStockRepository(settings)
    
    @staticmethod
    def create_market_data_repository(repo_type='s3', settings=None):
        """Create a market data repository instance."""
        settings = settings or RepositoryFactory.create_settings()
        
        if repo_type == 's3':
            return S3MarketDataRepository(settings)
        else:
            # Por enquanto, apenas a implementação S3 está disponível
            return S3MarketDataRepository(settings)
    
    @staticmethod
    def create_market_data_service(service_type='yahoo', settings=None):
        """Create a market data service instance."""
        settings = settings or RepositoryFactory.create_settings()
        
        if service_type == 'alpha':
            return AlphaVantageAdapter(settings)
        else:
            return YahooFinanceAdapter(settings)
    
    @staticmethod
    def create_data_processing_service(service_type='spark'):
        """Create a data processing service instance."""
        logger.info(f"Criando serviço de processamento do tipo: {service_type}")
        try:
            if service_type == 'pandas':
                return PandasDataProcessingService()
            elif service_type == 'spark':
                return SparkDataProcessingService()
            else:
                # O padrão agora é Spark para melhor escalabilidade
                logger.warning(f"Tipo de serviço '{service_type}' desconhecido, usando Spark como padrão")
                return SparkDataProcessingService()
        except Exception as e:
            logger.error(f"Erro ao criar serviço de processamento {service_type}: {str(e)}")
            logger.warning("Retornando para implementação Pandas devido a erro na inicialização do Spark")
            return PandasDataProcessingService()
    
    @staticmethod
    def create_observability_service(settings=None):
        """Create an observability service instance."""
        settings = settings or RepositoryFactory.create_settings()
        return AWSObservabilityService(settings)
    
    @staticmethod
    def create_data_masking_service(salt=None):
        """Create a data masking service instance."""
        return SimpleDataMaskingService(salt)