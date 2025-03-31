# src/interfaces/factories/lakehouse_factory.py
import logging
import boto3

from src.application.use_cases.bronze_layer import LoadToBronzeLayerUseCase
from src.application.use_cases.silver_layer import ProcessToSilverLayerUseCase
from src.application.use_cases.gold_layer import AggregateToGoldLayerUseCase
from src.infrastructure.config.settings import Settings
from src.infrastructure.services.aws_observability_service import AWSObservabilityService
from src.infrastructure.services.pandas_data_processing_service import PandasDataProcessingService
from src.infrastructure.services.spark_data_processing_service import SparkDataProcessingService

logger = logging.getLogger(__name__)

class LakehouseFactory:
    """Factory para criação de casos de uso relacionados ao Data Lakehouse."""
    
    @staticmethod
    def create_bronze_use_case(settings=None, observability_service=None):
        """Cria o caso de uso para a camada bronze."""
        settings = settings or Settings()
        observability_service = observability_service or AWSObservabilityService(settings)
        
        return LoadToBronzeLayerUseCase(
            bucket_name=settings.S3_DATA_BUCKET,
            observability_service=observability_service
        )
    
    @staticmethod
    def create_silver_use_case(settings=None, data_processing_service=None, observability_service=None):
        """Cria o caso de uso para a camada prata."""
        settings = settings or Settings()
        observability_service = observability_service or AWSObservabilityService(settings)
        
        # Tentar usar Spark primeiro, se falhar, usar Pandas
        if data_processing_service is None:
            try:
                logger.info("Tentando criar serviço de processamento Spark")
                data_processing_service = SparkDataProcessingService()
            except Exception as e:
                logger.warning(f"Não foi possível inicializar o Spark: {str(e)}. Usando Pandas.")
                data_processing_service = PandasDataProcessingService()
        
        return ProcessToSilverLayerUseCase(
            bucket_name=settings.S3_DATA_BUCKET,
            data_processing_service=data_processing_service,
            observability_service=observability_service
        )
    
    @staticmethod
    def create_gold_use_case(settings=None, observability_service=None):
        """Cria o caso de uso para a camada ouro."""
        settings = settings or Settings()
        observability_service = observability_service or AWSObservabilityService(settings)
        
        return AggregateToGoldLayerUseCase(
            bucket_name=settings.S3_DATA_BUCKET,
            observability_service=observability_service
        )
    
    @staticmethod
    def create_complete_lakehouse_pipeline(settings=None, use_spark=True):
        """
        Cria todo o pipeline de Lakehouse (Bronze, Prata e Ouro).
        
        Args:
            settings: Configurações da aplicação
            use_spark: Se deve usar Spark para processamento ou Pandas
            
        Returns:
            tuple: (bronze_use_case, silver_use_case, gold_use_case)
        """
        settings = settings or Settings()
        observability_service = AWSObservabilityService(settings)
        
        # Escolher o serviço de processamento
        if use_spark:
            try:
                data_processing_service = SparkDataProcessingService()
            except Exception as e:
                logger.warning(f"Não foi possível inicializar o Spark: {str(e)}. Usando Pandas.")
                data_processing_service = PandasDataProcessingService()
        else:
            data_processing_service = PandasDataProcessingService()
        
        # Criar os casos de uso
        bronze_use_case = LoadToBronzeLayerUseCase(
            bucket_name=settings.S3_DATA_BUCKET,
            observability_service=observability_service
        )
        
        silver_use_case = ProcessToSilverLayerUseCase(
            bucket_name=settings.S3_DATA_BUCKET,
            data_processing_service=data_processing_service,
            observability_service=observability_service
        )
        
        gold_use_case = AggregateToGoldLayerUseCase(
            bucket_name=settings.S3_DATA_BUCKET,
            observability_service=observability_service
        )
        
        return bronze_use_case, silver_use_case, gold_use_case