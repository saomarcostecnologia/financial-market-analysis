# src/application/use_cases/silver_layer.py
import io
import logging
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
import boto3

from src.domain.interfaces.services import ObservabilityService, DataProcessingService
from src.infrastructure.config.data_lake_settings import DataLakeSettings

class ProcessToSilverLayerUseCase:
    """Caso de uso para processar dados da camada bronze para prata."""
    
    def __init__(
        self,
        bucket_name: str,
        data_processing_service: DataProcessingService,
        observability_service: ObservabilityService,
        s3_client=None
    ):
        self.logger = logging.getLogger(__name__)
        self.bucket_name = bucket_name
        self.data_processing_service = data_processing_service
        self.observability_service = observability_service
        self.s3_client = s3_client or boto3.client('s3')
    
    def process_stock_data(self, ticker: str, bronze_key: str) -> str:
        """
        Processa dados da camada bronze para prata.
        
        Args:
            ticker: Símbolo da ação
            bronze_key: Chave S3 do arquivo na camada bronze
            
        Returns:
            str: Chave S3 onde os dados processados foram salvos
        """
        try:
            self.observability_service.log_event(
                "silver_layer_process_started",
                {
                    "ticker": ticker,
                    "bronze_key": bronze_key
                }
            )
            
            # Ler dados da camada bronze
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=bronze_key)
            bronze_data = pd.read_parquet(io.BytesIO(response['Body'].read()))
            
            # Processar dados
            processed_data = self._clean_and_standardize(bronze_data)
            
            # Calcular indicadores técnicos via serviço de processamento
            # Converter para o formato esperado pelo serviço
            data_for_processing = []
            for _, row in processed_data.iterrows():
                data_for_processing.append({
                    "timestamp": row['timestamp'].isoformat() if 'timestamp' in processed_data.columns else datetime.now().isoformat(),
                    "open": float(row['open']) if 'open' in processed_data.columns else None,
                    "high": float(row['high']) if 'high' in processed_data.columns else None,
                    "low": float(row['low']) if 'low' in processed_data.columns else None,
                    "close": float(row['close']) if 'close' in processed_data.columns else None,
                    "volume": int(row['volume']) if 'volume' in processed_data.columns else None,
                    "adjusted_close": float(row['adjusted_close']) if 'adjusted_close' in processed_data.columns else None
                })
            
            # Processar dados
            batch_id = f"{ticker}_{datetime.now().strftime('%Y%m%d')}"
            processed_results = self.data_processing_service.process_batch_data(batch_id, data_for_processing)
            
            # Adicionar resultados ao DataFrame
            if 'sma_20' in processed_results:
                sma_data = pd.DataFrame(processed_results['sma_20'])
                if len(sma_data) > 0:
                    sma_data['timestamp'] = pd.to_datetime(sma_data['timestamp'])
                    processed_data = pd.merge(processed_data, sma_data.rename(columns={'value': 'sma_20'}), on='timestamp', how='left')
            
            if 'rsi_14' in processed_results:
                rsi_data = pd.DataFrame(processed_results['rsi_14'])
                if len(rsi_data) > 0:
                    rsi_data['timestamp'] = pd.to_datetime(rsi_data['timestamp'])
                    processed_data = pd.merge(processed_data, rsi_data.rename(columns={'value': 'rsi_14'}), on='timestamp', how='left')
            
            # Definir caminho na camada prata
            now = datetime.now()
            path = DataLakeSettings.get_silver_path(ticker, "prices", now)
            filename = f"{ticker}_processed_{now.strftime('%Y%m%d')}.parquet"
            full_key = f"{path}{filename}"
            
            # Salvar no S3
            parquet_buffer = io.BytesIO()
            processed_data.to_parquet(parquet_buffer)
            parquet_buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_key,
                Body=parquet_buffer.getvalue()
            )
            
            # Salvar indicadores separadamente para facilitar análises futuras
            if processed_results:
                indicators_key = f"{path}{ticker}_indicators_{now.strftime('%Y%m%d')}.json"
                
                import json
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=indicators_key,
                    Body=json.dumps(processed_results),
                    ContentType="application/json"
                )
            
            self.observability_service.log_event(
                "silver_layer_process_completed",
                {
                    "ticker": ticker,
                    "bronze_key": bronze_key,
                    "silver_key": full_key,
                    "rows": len(processed_data),
                    "indicators_calculated": list(processed_results.keys()) if processed_results else []
                }
            )
            
            return full_key
            
        except Exception as e:
            self.logger.error(f"Erro ao processar dados para camada prata: {str(e)}")
            self.observability_service.log_event(
                "silver_layer_process_failed",
                {
                    "ticker": ticker,
                    "bronze_key": bronze_key,
                    "error": str(e)
                }
            )
            raise
    
    def _clean_and_standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e padroniza dados para a camada prata.
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame com dados limpos e padronizados
        """
        # Criar cópia para não modificar o original
        df_clean = df.copy()
        
        # Padronizar nomes de colunas
        column_mapping = {
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Adj Close': 'adjusted_close'
        }
        df_clean = df_clean.rename(columns={k: v for k, v in column_mapping.items() if k in df_clean.columns})
        
        # Garantir tipos de dados corretos
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
        
        # Lidar com valores ausentes
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'adjusted_close']
        for col in [c for c in numeric_cols if c in df_clean.columns]:
            # Preencher valores ausentes com método forward fill (último valor válido)
            df_clean[col] = df_clean[col].fillna(method='ffill')
        
        # Remover duplicatas
        df_clean = df_clean.drop_duplicates()
        
        # Ordenar por timestamp
        if 'timestamp' in df_clean.columns:
            df_clean = df_clean.sort_values('timestamp')
        
        return df_clean