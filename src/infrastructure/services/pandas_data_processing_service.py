# src/infrastructure/services/pandas_data_processing_service.py
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd
import numpy as np

from src.domain.interfaces.services import DataProcessingService


class PandasDataProcessingService(DataProcessingService):
    """Implementation of DataProcessingService using pandas for financial data analysis."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_batch_data(self, batch_id: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of data to calculate technical indicators."""
        try:
            self.logger.info(f"Processing batch {batch_id} with {len(data)} records")
            
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(data)
            
            # Make sure timestamp is datetime
            if 'timestamp' in df.columns:
                if isinstance(df['timestamp'].iloc[0], str):
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Sort by timestamp
                df = df.sort_values('timestamp')
            
            # Calculate technical indicators
            result = {}
            
            # Moving Averages
            if len(df) >= 5:
                result['sma_5'] = self._calculate_sma(df, 5)
            if len(df) >= 20:
                result['sma_20'] = self._calculate_sma(df, 20)
            if len(df) >= 50:
                result['sma_50'] = self._calculate_sma(df, 50)
            if len(df) >= 5:
                result['ema_5'] = self._calculate_ema(df, 5)
            if len(df) >= 20:
                result['ema_20'] = self._calculate_ema(df, 20)
            
            # Relative Strength Index
            if len(df) >= 14:
                result['rsi_14'] = self._calculate_rsi(df, 14)
            
            # Bollinger Bands
            if len(df) >= 20:
                result['bollinger_bands'] = self._calculate_bollinger_bands(df, 20, 2)
            
            # MACD
            if len(df) >= 26:
                result['macd'] = self._calculate_macd(df, 12, 26, 9)
            
            # Basic Statistics
            result['statistics'] = self._calculate_statistics(df)
            
            # Volatility
            if len(df) >= 2:
                result['volatility'] = self._calculate_volatility(df)
            
            # Trends
            if len(df) >= 20:
                result['trends'] = self._detect_trends(df)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing batch data: {str(e)}")
            # Return empty result on error
            return {"error": str(e)}
    
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
    
    def _calculate_sma(self, df: pd.DataFrame, window: int) -> List[Dict[str, Any]]:
        """Calculate Simple Moving Average."""
        if len(df) < window or 'close' not in df.columns or 'timestamp' not in df.columns:
            return []
        
        # Calculate SMA
        df_result = df.copy()
        df_result['sma'] = df['close'].rolling(window=window).mean()
        
        # Convert to list of dicts
        result = []
        for _, row in df_result.dropna().iterrows():
            result.append({
                'timestamp': row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp'],
                'value': float(row['sma'])
            })
        
        return result
    
    def _calculate_ema(self, df: pd.DataFrame, window: int) -> List[Dict[str, Any]]:
        """Calculate Exponential Moving Average."""
        if len(df) < window or 'close' not in df.columns or 'timestamp' not in df.columns:
            return []
        
        # Calculate EMA
        df_result = df.copy()
        df_result['ema'] = df['close'].ewm(span=window, adjust=False).mean()
        
        # Convert to list of dicts
        result = []
        for _, row in df_result.iterrows():
            result.append({
                'timestamp': row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp'],
                'value': float(row['ema'])
            })
        
        return result
    
    def _calculate_rsi(self, df: pd.DataFrame, window: int) -> List[Dict[str, Any]]:
        """Calculate Relative Strength Index."""
        if len(df) < window + 1 or 'close' not in df.columns or 'timestamp' not in df.columns:
            return []
        
        # Calculate price changes
        df_result = df.copy()
        df_result['change'] = df_result['close'].diff()
        
        # Calculate gains and losses
        df_result['gain'] = df_result['change'].apply(lambda x: max(x, 0))
        df_result['loss'] = df_result['change'].apply(lambda x: abs(min(x, 0)))
        
        # Calculate average gains and losses
        df_result['avg_gain'] = df_result['gain'].rolling(window=window).mean()
        df_result['avg_loss'] = df_result['loss'].rolling(window=window).mean()
        
        # Calculate RS and RSI
        df_result['rs'] = df_result['avg_gain'] / df_result['avg_loss'].replace(0, np.nan)
        df_result['rsi'] = 100 - (100 / (1 + df_result['rs']))
        
        # Convert to list of dicts
        result = []
        for _, row in df_result.dropna().iterrows():
            result.append({
                'timestamp': row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp'],
                'value': float(row['rsi'])
            })
        
        return result
    
    def _calculate_bollinger_bands(self, df: pd.DataFrame, window: int, std_dev: float) -> Dict[str, List[Dict[str, Any]]]:
        """Calculate Bollinger Bands."""
        if len(df) < window or 'close' not in df.columns or 'timestamp' not in df.columns:
            return {'upper': [], 'middle': [], 'lower': []}
        
        # Calculate SMA (middle band)
        df_result = df.copy()
        df_result['middle'] = df['close'].rolling(window=window).mean()
        
        # Calculate standard deviation
        df_result['std'] = df['close'].rolling(window=window).std()
        
        # Calculate upper and lower bands
        df_result['upper'] = df_result['middle'] + (df_result['std'] * std_dev)
        df_result['lower'] = df_result['middle'] - (df_result['std'] * std_dev)
        
        # Convert to dict of lists
        result = {
            'upper': [],
            'middle': [],
            'lower': []
        }
        
        for _, row in df_result.dropna().iterrows():
            timestamp = row['timestamp'].isoformat() if isinstance(row['timestamp'], datetime) else row['timestamp']
            
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
    
    def _calculate_macd(self, df: pd.DataFrame, fast_period: int, slow_period: int, signal_period: int) -> Dict[str, List[Dict[str, Any]]]:
        """Calculate Moving Average Convergence Divergence."""
        if len(df) < max(fast_period, slow_period, signal_period) or 'close' not in df.columns or 'timestamp' not in df.columns:
            return {'macd': [], 'signal': [], 'histogram': []}
        
        # Calculate EMAs
        df_result = df.copy()
        df_result['ema_fast'] = df['close'].ewm(span=fast_period, adjust=False).mean()
        df_result['ema_slow'] = df['close'].ewm(span=slow_period, adjust=False).mean()
        
        # Calculate MACD line
        df_result['macd'] = df_result['ema_fast'] - df_result['ema_slow']
        
        # Calculate signal line
        df_result['signal'] = df_result['macd'].ewm(span=signal_period, adjust=False).mean()
        
        # Calculate histogram
        df_result['histogram'] = df_result['macd'] - df_result['signal']
        
        # Convert to dict of lists
        result = {
            'macd': [],
            'signal': [],
            'histogram': []
        }
        
        for _, row in df_result.dropna().iterrows():
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
    
    def _calculate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate basic statistics for the data."""
        if 'close' not in df.columns or 'timestamp' not in df.columns:
            return {}
        
        stats = {}
        
        # Date range
        if len(df) > 0:
            stats['start_date'] = df['timestamp'].min().isoformat() if isinstance(df['timestamp'].min(), datetime) else df['timestamp'].min()
            stats['end_date'] = df['timestamp'].max().isoformat() if isinstance(df['timestamp'].max(), datetime) else df['timestamp'].max()
            stats['days'] = (df['timestamp'].max() - df['timestamp'].min()).days if isinstance(df['timestamp'].min(), datetime) else None
        
        # Price statistics
        stats['min_price'] = float(df['close'].min()) if len(df) > 0 else None
        stats['max_price'] = float(df['close'].max()) if len(df) > 0 else None
        stats['avg_price'] = float(df['close'].mean()) if len(df) > 0 else None
        
        # Volume statistics
        if 'volume' in df.columns:
            stats['min_volume'] = int(df['volume'].min()) if len(df) > 0 else None
            stats['max_volume'] = int(df['volume'].max()) if len(df) > 0 else None
            stats['avg_volume'] = float(df['volume'].mean()) if len(df) > 0 else None
        
        # Return statistics
        if len(df) > 1:
            df_temp = df.copy()
            df_temp['return'] = df_temp['close'].pct_change()
            stats['avg_daily_return'] = float(df_temp['return'].mean()) if len(df_temp) > 1 else None
            stats['std_daily_return'] = float(df_temp['return'].std()) if len(df_temp) > 1 else None
            
            # Compute total return
            first_price = df_temp['close'].iloc[0]
            last_price = df_temp['close'].iloc[-1]
            stats['total_return'] = float((last_price - first_price) / first_price)
        
        return stats
    
    def _calculate_volatility(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate volatility metrics."""
        if len(df) < 2 or 'close' not in df.columns:
            return {}
        
        volatility = {}
        
        # Daily returns
        df_temp = df.copy()
        df_temp['return'] = df_temp['close'].pct_change()
        
        # Daily volatility (standard deviation of returns)
        volatility['daily'] = float(df_temp['return'].std())
        
        # Annualized volatility (assuming 252 trading days per year)
        volatility['annualized'] = float(df_temp['return'].std() * np.sqrt(252))
        
        # Average True Range (ATR) for 14 days
        if all(col in df.columns for col in ['high', 'low', 'close']):
            df_temp['previous_close'] = df_temp['close'].shift(1)
            df_temp['tr1'] = df_temp['high'] - df_temp['low']
            df_temp['tr2'] = abs(df_temp['high'] - df_temp['previous_close'])
            df_temp['tr3'] = abs(df_temp['low'] - df_temp['previous_close'])
            df_temp['true_range'] = df_temp[['tr1', 'tr2', 'tr3']].max(axis=1)
            volatility['atr_14'] = float(df_temp['true_range'].rolling(window=14).mean().iloc[-1]) if len(df_temp) >= 14 else None
        
        return volatility
    
    def _detect_trends(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect trends in the data."""
        if len(df) < 20 or 'close' not in df.columns:
            return {}
        
        trends = {}
        
        # Compute moving averages
        df_temp = df.copy()
        df_temp['sma_20'] = df_temp['close'].rolling(window=20).mean()
        if len(df) >= 50:
            df_temp['sma_50'] = df_temp['close'].rolling(window=50).mean()
        
        # Determine current trend based on price vs. moving averages
        last_row = df_temp.iloc[-1]
        if len(df_temp) >= 50 and not pd.isna(last_row.get('sma_50')):
            if last_row['close'] > last_row['sma_20'] and last_row['sma_20'] > last_row['sma_50']:
                trends['current'] = 'bullish'
            elif last_row['close'] < last_row['sma_20'] and last_row['sma_20'] < last_row['sma_50']:
                trends['current'] = 'bearish'
            else:
                trends['current'] = 'sideways'
            
            # Calculate trend strength
            trend_strength = abs(last_row['close'] - last_row['sma_50']) / last_row['sma_50']
            trends['strength'] = float(trend_strength)
        elif len(df_temp) >= 20:
            if last_row['close'] > last_row['sma_20']:
                trends['current'] = 'bullish'
            elif last_row['close'] < last_row['sma_20']:
                trends['current'] = 'bearish'
            else:
                trends['current'] = 'sideways'
                
            # Calculate trend strength with SMA20
            trend_strength = abs(last_row['close'] - last_row['sma_20']) / last_row['sma_20']
            trends['strength'] = float(trend_strength)
        
        # Identify support and resistance levels
        if len(df) >= 30:
            pivots = self._find_pivot_points(df_temp)
            supports = [p for p in pivots if p['type'] == 'support']
            resistances = [p for p in pivots if p['type'] == 'resistance']
            
            if supports:
                trends['support_levels'] = [float(s['price']) for s in supports[-3:]]
            
            if resistances:
                trends['resistance_levels'] = [float(r['price']) for r in resistances[-3:]]
        
        return trends
    
    def _find_pivot_points(self, df: pd.DataFrame, window: int = 5) -> List[Dict[str, Any]]:
        """Find pivot points (support and resistance) in the price data."""
        if len(df) < window * 2 + 1 or 'close' not in df.columns or 'timestamp' not in df.columns:
            return []
        
        pivots = []
        
        for i in range(window, len(df) - window):
            is_pivot_high = True
            is_pivot_low = True
            
            # Check if this is a pivot high
            for j in range(i - window, i + window + 1):
                if j == i:
                    continue
                
                if df['close'].iloc[j] > df['close'].iloc[i]:
                    is_pivot_high = False
                    break
            
            # Check if this is a pivot low
            for j in range(i - window, i + window + 1):
                if j == i:
                    continue
                
                if df['close'].iloc[j] < df['close'].iloc[i]:
                    is_pivot_low = False
                    break
            
            # Record pivot point
            if is_pivot_high:
                pivots.append({
                    'timestamp': df['timestamp'].iloc[i].isoformat() if isinstance(df['timestamp'].iloc[i], datetime) else df['timestamp'].iloc[i],
                    'price': float(df['close'].iloc[i]),
                    'type': 'resistance'
                })
            elif is_pivot_low:
                pivots.append({
                    'timestamp': df['timestamp'].iloc[i].isoformat() if isinstance(df['timestamp'].iloc[i], datetime) else df['timestamp'].iloc[i],
                    'price': float(df['close'].iloc[i]),
                    'type': 'support'
                })
        
        return pivots