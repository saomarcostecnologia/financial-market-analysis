# src/application/use_cases/gold_layer.py
import io
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd
import numpy as np
import boto3

from src.domain.interfaces.services import ObservabilityService
from src.infrastructure.config.data_lake_settings import DataLakeSettings

class AggregateToGoldLayerUseCase:
    """Caso de uso para agregar dados da camada prata para ouro."""
    
    def __init__(
        self,
        bucket_name: str,
        observability_service: ObservabilityService,
        s3_client=None
    ):
        self.logger = logging.getLogger(__name__)
        self.bucket_name = bucket_name
        self.observability_service = observability_service
        self.s3_client = s3_client or boto3.client('s3')
    
    def aggregate_stock_data(self, ticker: str, silver_keys: List[str]) -> Dict[str, str]:
        """
        Agrega dados da camada prata para ouro.
        
        Args:
            ticker: Símbolo da ação
            silver_keys: Lista de chaves S3 dos arquivos na camada prata
            
        Returns:
            Dict[str, str]: Dicionário com as chaves S3 onde os dados agregados foram salvos
        """
        try:
            self.observability_service.log_event(
                "gold_layer_aggregate_started",
                {
                    "ticker": ticker,
                    "silver_keys_count": len(silver_keys)
                }
            )
            
            # Ler todos os dados da camada prata
            all_data = []
            for key in silver_keys:
                try:
                    response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                    df = pd.read_parquet(io.BytesIO(response['Body'].read()))
                    all_data.append(df)
                except Exception as e:
                    self.logger.warning(f"Erro ao ler dados da camada prata {key}: {str(e)}")
            
            if not all_data:
                raise ValueError("Nenhum dado encontrado para agregação")
                
            # Combinar todos os dados
            combined_data = pd.concat(all_data)
            
            # Calcular agregações
            monthly_data = self._calculate_monthly_aggregations(combined_data)
            statistics = self._calculate_statistics(combined_data)
            
            # Definir caminho na camada ouro
            now = datetime.now()
            path = DataLakeSettings.get_gold_path(ticker, "analytics")
            result_keys = {}
            
            # Salvar dados mensais
            if monthly_data is not None and not monthly_data.empty:
                monthly_key = f"{path}{ticker}_monthly_{now.strftime('%Y%m%d')}.parquet"
                parquet_buffer = io.BytesIO()
                monthly_data.to_parquet(parquet_buffer)
                parquet_buffer.seek(0)
                
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=monthly_key,
                    Body=parquet_buffer.getvalue()
                )
                result_keys['monthly'] = monthly_key
            
            # Salvar estatísticas
            if statistics:
                stats_key = f"{path}{ticker}_stats_{now.strftime('%Y%m%d')}.json"
                
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=stats_key,
                    Body=json.dumps(statistics),
                    ContentType="application/json"
                )
                result_keys['statistics'] = stats_key
            
            self.observability_service.log_event(
                "gold_layer_aggregate_completed",
                {
                    "ticker": ticker,
                    "silver_keys_count": len(silver_keys),
                    "gold_keys": result_keys
                }
            )
            
            return result_keys
            
        except Exception as e:
            self.logger.error(f"Erro ao agregar dados para camada ouro: {str(e)}")
            self.observability_service.log_event(
                "gold_layer_aggregate_failed",
                {
                    "ticker": ticker,
                    "error": str(e)
                }
            )
            raise
    
    def _calculate_monthly_aggregations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula agregações mensais para a camada ouro.
        
        Args:
            df: DataFrame com dados de preços
            
        Returns:
            DataFrame com dados agregados por mês
        """
        try:
            # Verificar se temos as colunas necessárias
            required_cols = ['timestamp', 'open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_cols):
                self.logger.warning(f"Colunas necessárias ausentes para agregação mensal: {[col for col in required_cols if col not in df.columns]}")
                return None
                
            # Converter timestamp para datetime se necessário
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Extrair ano e mês
                df['year_month'] = df['timestamp'].dt.strftime('%Y-%m')
                
                # Agrupar por ano-mês
                monthly_data = df.groupby('year_month').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum' if 'volume' in df.columns else 'first',
                    'timestamp': 'first'  # Manter para referência
                }).reset_index()
                
                # Calcular retornos mensais
                monthly_data['monthly_return'] = monthly_data['close'].pct_change() * 100
                
                # Calcular média móvel de 3 meses
                monthly_data['ma_3m'] = monthly_data['close'].rolling(window=3).mean()
                
                # Calcular volatilidade mensal (se tivermos dados diários suficientes)
                volatility = df.groupby('year_month')['close'].std()
                monthly_data['volatility'] = monthly_data['year_month'].map(volatility)
                
                return monthly_data
            else:
                return None
        except Exception as e:
            self.logger.error(f"Erro ao calcular agregações mensais: {str(e)}")
            return None
    
    def _calculate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calcula estatísticas gerais para a camada ouro.
        
        Args:
            df: DataFrame com dados de preços
            
        Returns:
            Dicionário com estatísticas calculadas
        """
        try:
            stats = {}
            
            # Estatísticas básicas
            if 'close' in df.columns:
                stats['price_stats'] = {
                    'min': float(df['close'].min()),
                    'max': float(df['close'].max()),
                    'mean': float(df['close'].mean()),
                    'median': float(df['close'].median()),
                    'std': float(df['close'].std())
                }
            
            # Estatísticas de retorno
            if 'close' in df.columns and len(df) > 1:
                df_sorted = df.sort_values('timestamp') if 'timestamp' in df.columns else df
                df_sorted['return'] = df_sorted['close'].pct_change() * 100
                
                stats['return_stats'] = {
                    'min_daily_return': float(df_sorted['return'].min()),
                    'max_daily_return': float(df_sorted['return'].max()),
                    'mean_daily_return': float(df_sorted['return'].mean()),
                    'volatility': float(df_sorted['return'].std()),
                    'annualized_volatility': float(df_sorted['return'].std() * np.sqrt(252))
                }
                
                # Retorno total
                first_price = df_sorted['close'].iloc[0]
                last_price = df_sorted['close'].iloc[-1]
                total_return = ((last_price - first_price) / first_price) * 100
                stats['total_return_pct'] = float(total_return)
            
            # Estatísticas de volume
            if 'volume' in df.columns:
                stats['volume_stats'] = {
                    'min': int(df['volume'].min()),
                    'max': int(df['volume'].max()),
                    'mean': float(df['volume'].mean()),
                    'median': float(df['volume'].median()),
                    'std': float(df['volume'].std())
                }
            
            # Informações temporais
            if 'timestamp' in df.columns:
                stats['time_range'] = {
                    'start_date': df['timestamp'].min().strftime('%Y-%m-%d'),
                    'end_date': df['timestamp'].max().strftime('%Y-%m-%d'),
                    'days': (df['timestamp'].max() - df['timestamp'].min()).days,
                    'trading_days': len(df)
                }
            
            # Tendência atual
            if 'close' in df.columns and 'sma_20' in df.columns and len(df) > 0:
                last_row = df.iloc[-1]
                if pd.notna(last_row.get('sma_20')):
                    if last_row['close'] > last_row['sma_20']:
                        trend = 'bullish'
                    else:
                        trend = 'bearish'
                        
                    stats['current_trend'] = {
                        'direction': trend,
                        'strength': float(abs((last_row['close'] - last_row['sma_20']) / last_row['sma_20']) * 100)
                    }
            
            return stats
        except Exception as e:
            self.logger.error(f"Erro ao calcular estatísticas: {str(e)}")
            return {}