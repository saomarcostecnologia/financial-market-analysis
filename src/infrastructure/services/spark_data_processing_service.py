# src/infrastructure/services/spark_data_processing_service.py
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, window, avg, stddev, lag, expr, max as spark_max, min as spark_min
from pyspark.sql.types import DoubleType, StringType, StructType, StructField, TimestampType, IntegerType
import pandas as pd
import numpy as np

from src.domain.interfaces.services import DataProcessingService


class SparkDataProcessingService(DataProcessingService):
    """Implementation of DataProcessingService using Apache Spark for scalable financial data analysis."""
    
    def __init__(self, spark_session=None):
        self.logger = logging.getLogger(__name__)
        self.spark = spark_session or self._create_spark_session()
    
    def _create_spark_session(self):
        """Create a Spark session."""
        try:
            return SparkSession.builder \
                .appName("FinancialMarketAnalysis") \
                .config("spark.sql.session.timeZone", "UTC") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .getOrCreate()
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            # Fall back to local session if cluster connection fails
            return SparkSession.builder \
                .appName("FinancialMarketAnalysis") \
                .master("local[*]") \
                .config("spark.sql.session.timeZone", "UTC") \
                .getOrCreate()
    
    def process_batch_data(self, batch_id: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of data to calculate technical indicators."""
        try:
            self.logger.info(f"Processing batch {batch_id} with {len(data)} records using Spark")
            
            # Define schema for better performance
            schema = StructType([
                StructField("timestamp", TimestampType(), True),
                StructField("open", DoubleType(), True),
                StructField("high", DoubleType(), True),
                StructField("low", DoubleType(), True),
                StructField("close", DoubleType(), True),
                StructField("volume", IntegerType(), True),
                StructField("adjusted_close", DoubleType(), True)
            ])
            
            # Handle timestamp conversion in data
            for record in data:
                if isinstance(record.get('timestamp'), str):
                    try:
                        record['timestamp'] = datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        pass
            
            # Convert data to Spark DataFrame
            df_pd = pd.DataFrame(data)
            df = self.spark.createDataFrame(df_pd, schema=schema)
            
            # Ensure timestamp column is sorted
            df = df.orderBy("timestamp")
            
            # Calculate technical indicators
            result = {}
            
            # Moving Averages
            if df.count() >= 5:
                result['sma_5'] = self._calculate_sma(df, 5)
            if df.count() >= 20:
                result['sma_20'] = self._calculate_sma(df, 20)
            if df.count() >= 50:
                result['sma_50'] = self._calculate_sma(df, 50)
            if df.count() >= 5:
                result['ema_5'] = self._calculate_ema(df, 5)
            if df.count() >= 20:
                result['ema_20'] = self._calculate_ema(df, 20)
            
            # Relative Strength Index
            if df.count() >= 14:
                result['rsi_14'] = self._calculate_rsi(df, 14)
            
            # Bollinger Bands
            if df.count() >= 20:
                result['bollinger_bands'] = self._calculate_bollinger_bands(df, 20, 2)
            
            # MACD
            if df.count() >= 26:
                result['macd'] = self._calculate_macd(df, 12, 26, 9)
            
            # Basic Statistics
            result['statistics'] = self._calculate_statistics(df)
            
            # Volatility
            if df.count() >= 2:
                result['volatility'] = self._calculate_volatility(df)
            
            # Trends
            if df.count() >= 20:
                result['trends'] = self._detect_trends(df)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing batch data with Spark: {str(e)}")
            # Return empty result on error
            return {"error": str(e)}
        finally:
            # Don't stop the SparkSession as it's expensive to recreate
            pass
    
    def process_stream_data(self, stream_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process streaming data."""
        try:
            self.logger.info(f"Processing stream data: {stream_data}")
            
            # For stream data, we'll just do some basic validation and enrichment
            result = stream_data.copy()
            
            # Add timestamp if not present
            if 'timestamp' not in result:
                result['timestamp'] = datetime.now().isoformat()
            
            # Calculate intraday basic indicators
            if all(key in result for key in ['open', 'high', 'low', 'close']):
                # Daily performance
                result['daily_performance'] = (result['close'] - result['open']) / result['open'] * 100
                
                # Range
                result['range'] = result['high'] - result['low']
                
                # Intraday volatility
                result['intraday_volatility'] = result['range'] / result['open'] * 100
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing stream data: {str(e)}")
            return None
    
    def _calculate_sma(self, df, window: int) -> List[Dict[str, Any]]:
        """Calculate Simple Moving Average using Spark window functions."""
        try:
            from pyspark.sql.window import Window
            import pyspark.sql.functions as F
            
            # Define the window specification
            windowSpec = Window.orderBy("timestamp").rowsBetween(-(window-1), 0)
            
            # Calculate SMA
            sma_df = df.withColumn(f"sma_{window}", F.avg("close").over(windowSpec))
            
            # Convert to list of dictionaries
            result = []
            
            # Collect only necessary columns to minimize data transfer
            rows = sma_df.select("timestamp", f"sma_{window}").collect()
            
            for row in rows:
                if row[f"sma_{window}"] is not None:
                    result.append({
                        'timestamp': row['timestamp'].isoformat(),
                        'value': float(row[f"sma_{window}"])
                    })
            
            return result
        except Exception as e:
            self.logger.error(f"Error calculating SMA: {str(e)}")
            return []
    
    def _calculate_ema(self, df, window: int) -> List[Dict[str, Any]]:
        """Calculate Exponential Moving Average."""
        try:
            # EMA requires more complex calculation in Spark
            # Converting to pandas for now - in production we would implement using Spark UDFs
            pd_df = df.toPandas()
            pd_df[f'ema_{window}'] = pd_df['close'].ewm(span=window, adjust=False).mean()
            
            # Convert back to result format
            result = []
            for idx, row in pd_df.iterrows():
                if not pd.isna(row[f'ema_{window}']):
                    result.append({
                        'timestamp': row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp'],
                        'value': float(row[f'ema_{window}'])
                    })
            
            return result
        except Exception as e:
            self.logger.error(f"Error calculating EMA: {str(e)}")
            return []
    
    def _calculate_rsi(self, df, window: int) -> List[Dict[str, Any]]:
        """Calculate Relative Strength Index."""
        try:
            from pyspark.sql.window import Window
            import pyspark.sql.functions as F
            
            # Define window specification for lag
            windowSpec = Window.orderBy("timestamp")
            
            # Calculate price changes
            rsi_df = df.withColumn("prev_close", F.lag("close", 1).over(windowSpec))
            rsi_df = rsi_df.withColumn("change", F.col("close") - F.col("prev_close"))
            
            # Calculate gains and losses
            rsi_df = rsi_df.withColumn("gain", F.when(F.col("change") > 0, F.col("change")).otherwise(0))
            rsi_df = rsi_df.withColumn("loss", F.when(F.col("change") < 0, -F.col("change")).otherwise(0))
            
            # Define window for rolling averages
            rollWindow = Window.orderBy("timestamp").rowsBetween(-(window-1), 0)
            
            # Calculate average gains and losses
            rsi_df = rsi_df.withColumn("avg_gain", F.avg("gain").over(rollWindow))
            rsi_df = rsi_df.withColumn("avg_loss", F.avg("loss").over(rollWindow))
            
            # Calculate RS and RSI
            rsi_df = rsi_df.withColumn(
                "rs", 
                F.when(F.col("avg_loss") == 0, 100).otherwise(F.col("avg_gain") / F.col("avg_loss"))
            )
            rsi_df = rsi_df.withColumn(
                "rsi", 
                F.when(F.col("avg_loss") == 0, 100).otherwise(100 - (100 / (1 + F.col("rs"))))
            )
            
            # Convert to list of dictionaries
            result = []
            rows = rsi_df.select("timestamp", "rsi").filter(F.col("rsi").isNotNull()).collect()
            
            for row in rows:
                result.append({
                    'timestamp': row['timestamp'].isoformat(),
                    'value': float(row['rsi'])
                })
            
            return result
        except Exception as e:
            self.logger.error(f"Error calculating RSI: {str(e)}")
            return []
    
    def _calculate_bollinger_bands(self, df, window: int, std_dev: float) -> Dict[str, List[Dict[str, Any]]]:
        """Calculate Bollinger Bands."""
        try:
            from pyspark.sql.window import Window
            import pyspark.sql.functions as F
            
            # Define window for calculations
            windowSpec = Window.orderBy("timestamp").rowsBetween(-(window-1), 0)
            
            # Calculate middle band (SMA)
            bb_df = df.withColumn("middle", F.avg("close").over(windowSpec))
            
            # Calculate standard deviation
            bb_df = bb_df.withColumn("std", F.stddev("close").over(windowSpec))
            
            # Calculate upper and lower bands
            bb_df = bb_df.withColumn("upper", F.col("middle") + (F.col("std") * std_dev))
            bb_df = bb_df.withColumn("lower", F.col("middle") - (F.col("std") * std_dev))
            
            # Convert to dict of lists
            result = {
                'upper': [],
                'middle': [],
                'lower': []
            }
            
            rows = bb_df.select("timestamp", "upper", "middle", "lower").filter(F.col("middle").isNotNull()).collect()
            
            for row in rows:
                timestamp = row['timestamp'].isoformat()
                
                result['upper'].append({
                    'timestamp': timestamp,
                    'value': float(row['upper'])
                })
                
                result['middle'].append({
                    'timestamp': timestamp,
                    'value': float(row['middle'])
                })
                
                result['lower'].append({
                    'timestamp': timestamp,
                    'value': float(row['lower'])
                })
            
            return result
        except Exception as e:
            self.logger.error(f"Error calculating Bollinger Bands: {str(e)}")
            return {'upper': [], 'middle': [], 'lower': []}
    
    def _calculate_macd(self, df, fast_period: int, slow_period: int, signal_period: int) -> Dict[str, List[Dict[str, Any]]]:
        """Calculate Moving Average Convergence Divergence."""
        try:
            # This calculation is more complex in Spark
            # For the MVP, we'll convert to pandas, calculate, and convert back
            pd_df = df.toPandas()
            
            # Calculate EMAs
            pd_df['ema_fast'] = pd_df['close'].ewm(span=fast_period, adjust=False).mean()
            pd_df['ema_slow'] = pd_df['close'].ewm(span=slow_period, adjust=False).mean()
            
            # Calculate MACD line
            pd_df['macd'] = pd_df['ema_fast'] - pd_df['ema_slow']
            
            # Calculate signal line
            pd_df['signal'] = pd_df['macd'].ewm(span=signal_period, adjust=False).mean()
            
            # Calculate histogram
            pd_df['histogram'] = pd_df['macd'] - pd_df['signal']
            
            # Convert to dict of lists
            result = {
                'macd': [],
                'signal': [],
                'histogram': []
            }
            
            for _, row in pd_df.dropna().iterrows():
                timestamp = row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp']
                
                result['macd'].append({
                    'timestamp': timestamp,
                    'value': float(row['macd'])
                })
                
                result['signal'].append({
                    'timestamp': timestamp,
                    'value': float(row['signal'])
                })
                
                result['histogram'].append({
                    'timestamp': timestamp,
                    'value': float(row['histogram'])
                })
            
            return result
        except Exception as e:
            self.logger.error(f"Error calculating MACD: {str(e)}")
            return {'macd': [], 'signal': [], 'histogram': []}
    
    def _calculate_statistics(self, df) -> Dict[str, Any]:
        """Calculate basic statistics for the data."""
        try:
            # Use Spark's built-in statistical functions
            count = df.count()
            
            if count == 0:
                return {}
            
            # Use Spark SQL for aggregate calculations
            stats_df = df.agg(
                spark_min("timestamp").alias("start_date"),
                spark_max("timestamp").alias("end_date"),
                spark_min("close").alias("min_price"),
                spark_max("close").alias("max_price"),
                avg("close").alias("avg_price")
            )
            
            if "volume" in df.columns:
                volume_stats = df.agg(
                    spark_min("volume").alias("min_volume"),
                    spark_max("volume").alias("max_volume"),
                    avg("volume").alias("avg_volume")
                ).first()
            else:
                volume_stats = None
            
            # Collect results
            stats_row = stats_df.first()
            
            stats = {
                "start_date": stats_row["start_date"].isoformat(),
                "end_date": stats_row["end_date"].isoformat(),
                "days": (stats_row["end_date"].date() - stats_row["start_date"].date()).days,
                "min_price": float(stats_row["min_price"]),
                "max_price": float(stats_row["max_price"]),
                "avg_price": float(stats_row["avg_price"])
            }
            
            # Add volume statistics if available
            if volume_stats:
                stats["min_volume"] = int(volume_stats["min_volume"])
                stats["max_volume"] = int(volume_stats["max_volume"])
                stats["avg_volume"] = float(volume_stats["avg_volume"])
            
            # Calculate returns
            if count > 1:
                # This requires pandas conversion for simplicity in MVP
                pd_df = df.toPandas()
                pd_df['return'] = pd_df['close'].pct_change()
                
                first_price = pd_df['close'].iloc[0]
                last_price = pd_df['close'].iloc[-1]
                
                stats['avg_daily_return'] = float(pd_df['return'].mean())
                stats['std_daily_return'] = float(pd_df['return'].std())
                stats['total_return'] = float((last_price - first_price) / first_price)
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error calculating statistics: {str(e)}")
            return {}
    
    def _calculate_volatility(self, df) -> Dict[str, Any]:
        """Calculate volatility metrics."""
        try:
            if df.count() < 2:
                return {}
            
            # Convert to pandas for volatility calculations in MVP
            pd_df = df.toPandas()
            pd_df['return'] = pd_df['close'].pct_change()
            
            volatility = {}
            
            # Daily volatility
            volatility['daily'] = float(pd_df['return'].std())
            
            # Annualized volatility
            volatility['annualized'] = float(pd_df['return'].std() * np.sqrt(252))
            
            # ATR calculation
            if all(col in df.columns for col in ['high', 'low', 'close']):
                pd_df['previous_close'] = pd_df['close'].shift(1)
                pd_df['tr1'] = pd_df['high'] - pd_df['low']
                pd_df['tr2'] = pd_df['high'] - pd_df['previous_close'].abs()
                pd_df['tr3'] = pd_df['low'] - pd_df['previous_close'].abs()
                pd_df['true_range'] = pd_df[['tr1', 'tr2', 'tr3']].max(axis=1)
                
                if len(pd_df) >= 14:
                    volatility['atr_14'] = float(pd_df['true_range'].rolling(window=14).mean().iloc[-1])
            
            return volatility
            
        except Exception as e:
            self.logger.error(f"Error calculating volatility: {str(e)}")
            return {}
    
    def _detect_trends(self, df) -> Dict[str, Any]:
        """Detect trends in the data."""
        try:
            if df.count() < 20:
                return {}
            
            # Convert to pandas for trend detection in MVP
            pd_df = df.toPandas()
            
            # Calculate moving averages
            pd_df['sma_20'] = pd_df['close'].rolling(window=20).mean()
            if len(pd_df) >= 50:
                pd_df['sma_50'] = pd_df['close'].rolling(window=50).mean()
            
            trends = {}
            
            # Determine current trend
            last_row = pd_df.iloc[-1]
            
            if len(pd_df) >= 50 and not pd.isna(last_row.get('sma_50')):
                if last_row['close'] > last_row['sma_20'] and last_row['sma_20'] > last_row['sma_50']:
                    trends['current'] = 'bullish'
                elif last_row['close'] < last_row['sma_20'] and last_row['sma_20'] < last_row['sma_50']:
                    trends['current'] = 'bearish'
                else:
                    trends['current'] = 'sideways'
                
                # Calculate trend strength
                trend_strength = abs(last_row['close'] - last_row['sma_50']) / last_row['sma_50']
                trends['strength'] = float(trend_strength)
            elif len(pd_df) >= 20:
                if last_row['close'] > last_row['sma_20']:
                    trends['current'] = 'bullish'
                elif last_row['close'] < last_row['sma_20']:
                    trends['current'] = 'bearish'
                else:
                    trends['current'] = 'sideways'
                    
                # Calculate trend strength with SMA20
                trend_strength = abs(last_row['close'] - last_row['sma_20']) / last_row['sma_20']
                trends['strength'] = float(trend_strength)
            
            return trends
            
        except Exception as e:
            self.logger.error(f"Error detecting trends: {str(e)}")
            return {}