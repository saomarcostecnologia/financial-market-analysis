# src/application/use_cases/bronze_layer.py
import io
import logging
from datetime import datetime
from typing import Dict, Any

import pandas as pd
import boto3

from src.domain.interfaces.services import ObservabilityService
from src.infrastructure.config.data_lake_settings import DataLakeSettings

class LoadToBronzeLayerUseCase:
    """Caso de uso para carregar dados na camada bronze."""
    
    def __init__(
        self,
        bucket_name: str,
        observability_service: ObservabilityService,
        s3_client=None
    ):
        self.logger = logging.getLogger(__name__)
        self.bucket_name = bucket_name
        self.observability_service = observability_service
        self.s3_client = s3_client or boto3.client('s3', region_name="sa-east-1")
    
    def load_stock_data(self, ticker: str, data_df: pd.DataFrame, data_type: str, timestamp: datetime = None) -> str:
        """
        Carrega dados de ações na camada bronze.
        
        Args:
            ticker: Símbolo da ação
            data_df: DataFrame com os dados
            data_type: Tipo de dados (prices, fundamentals, etc)
            timestamp: Timestamp dos dados (padrão: now)
            
        Returns:
            str: Chave S3 onde os dados foram salvos
        """
        if timestamp is None:
            timestamp = datetime.now()
            
        try:
            self.observability_service.log_event(
                "bronze_layer_load_started",
                {
                    "ticker": ticker,
                    "data_type": data_type,
                    "rows": len(data_df)
                }
            )
            
            # Definir caminho na camada bronze
            date = timestamp.date()
            path = DataLakeSettings.get_bronze_path(ticker, data_type, date)
            filename = f"{ticker}_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
            full_key = f"{path}{filename}"
            
            # Converter para parquet
            parquet_buffer = io.BytesIO()
            data_df.to_parquet(parquet_buffer)
            parquet_buffer.seek(0)
            
            # Salvar no S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_key,
                Body=parquet_buffer.getvalue()
            )
            
            self.observability_service.log_event(
                "bronze_layer_load_completed",
                {
                    "ticker": ticker,
                    "data_type": data_type,
                    "path": full_key,
                    "rows": len(data_df)
                }
            )
            
            return full_key
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar dados na camada bronze: {str(e)}")
            self.observability_service.log_event(
                "bronze_layer_load_failed",
                {
                    "ticker": ticker,
                    "data_type": data_type,
                    "error": str(e)
                }
            )
            raise