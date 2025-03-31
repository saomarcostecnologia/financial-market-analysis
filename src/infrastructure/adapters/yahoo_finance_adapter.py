# src/infrastructure/adapters/yahoo_finance_adapter.py
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import boto3
import yfinance as yf

from src.domain.entities.stock import StockPrice
from src.domain.interfaces.services import FinancialDataService
from src.infrastructure.config.settings import Settings
from src.infrastructure.services.data_cache_service import DataCacheService

class YahooFinanceAdapter(FinancialDataService):
    """Adapter for Yahoo Finance API."""
    
    def __init__(self, settings: Settings, ssm_client=None, cache_service=None):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.ssm_client = ssm_client or boto3.client('ssm', region_name=settings.AWS_REGION)
        self.cache = cache_service or DataCacheService(ttl_minutes=60)
    
    def get_stock_historical_prices(self, ticker: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Retrieve historical stock prices from Yahoo Finance."""
        try:
            self.logger.info(f"Fetching historical data for {ticker} from {start_date} to {end_date}")
            cache_key = f"yahoo_prices_{ticker}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

            cached_data = self.cache.get(cache_key)
            if cached_data:
                self.logger.info(f"Using cached data for {ticker} from {start_date} to {end_date}")
                return cached_data
            
            self.logger.info(f"Fetching historical data for {ticker} from {start_date} to {end_date}")
            
            # Get data from yfinance
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date)
            
            # Convert to StockPrice entities
            prices = []
            for index, row in df.iterrows():
                price = StockPrice(
                    timestamp=index.to_pydatetime(),
                    open=float(row["Open"]),
                    high=float(row["High"]),
                    low=float(row["Low"]),
                    close=float(row["Close"]),
                    volume=int(row["Volume"]),
                    adjusted_close=float(row["Close"])  # Yahoo Finance returns adjusted close
                )
                prices.append(price)
            
            self.cache.set(cache_key, prices)
            self.logger.info(f"Retrieved {len(prices)} price points for {ticker}")
            return prices
            
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {ticker}: {str(e)}")
            raise
    
    def get_stock_info(self, ticker: str) -> Dict[str, Any]:
        """Retrieve general information about a stock from Yahoo Finance."""
        try:
            self.logger.info(f"Fetching stock info for {ticker}")
            
            # Get data from yfinance
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # Extract relevant information
            result = {
                "ticker": ticker,
                "name": info.get("shortName", ticker),
                "exchange": info.get("exchange", "Unknown"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "currency": info.get("currency"),
                "market_cap": info.get("marketCap"),
                "country": info.get("country"),
                "website": info.get("website")
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching stock info for {ticker}: {str(e)}")
            raise


# src/infrastructure/adapters/alpha_vantage_adapter.py
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import boto3
import requests

from src.domain.entities.stock import StockPrice
from src.domain.interfaces.services import FinancialDataService
from src.infrastructure.config.settings import Settings


class AlphaVantageAdapter(FinancialDataService):
    """Adapter for Alpha Vantage API."""
    
    def __init__(self, settings: Settings, ssm_client=None):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.ssm_client = ssm_client or boto3.client('ssm')
        self.api_key = self._get_api_key()
        self.base_url = "https://www.alphavantage.co/query"
    
    def _get_api_key(self) -> str:
        """Retrieve API key from AWS SSM Parameter Store."""
        try:
            response = self.ssm_client.get_parameter(
                Name=self.settings.ALPHA_VANTAGE_API_KEY_PARAM,
                WithDecryption=True
            )
            return response['Parameter']['Value']
        except Exception as e:
            self.logger.error(f"Error retrieving Alpha Vantage API key: {str(e)}")
            raise
    
    def get_stock_historical_prices(self, ticker: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Retrieve historical stock prices from Alpha Vantage."""
        try:
            self.logger.info(f"Fetching historical data for {ticker} from {start_date} to {end_date}")
            
            # Make API request to Alpha Vantage
            params = {
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": ticker,
                "outputsize": "full",
                "apikey": self.api_key
            }
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                raise ValueError(f"Alpha Vantage API error: {data['Error Message']}")
            
            if "Time Series (Daily)" not in data:
                raise ValueError(f"Unexpected Alpha Vantage API response format")
            
            # Parse response
            time_series = data["Time Series (Daily)"]
            prices = []
            
            for date_str, values in time_series.items():
                date = datetime.strptime(date_str, "%Y-%m-%d")
                
                # Filter by date range
                if date < start_date or date > end_date:
                    continue
                
                price = StockPrice(
                    timestamp=date,
                    open=float(values["1. open"]),
                    high=float(values["2. high"]),
                    low=float(values["3. low"]),
                    close=float(values["4. close"]),
                    volume=int(values["6. volume"]),
                    adjusted_close=float(values["5. adjusted close"])
                )
                prices.append(price)
            
            self.logger.info(f"Retrieved {len(prices)} price points for {ticker}")
            return prices
            
        except Exception as e:
            self.logger.error(f"Error fetching historical data for {ticker}: {str(e)}")
            raise
    
    def get_stock_info(self, ticker: str) -> Dict[str, Any]:
        """Retrieve general information about a stock from Alpha Vantage."""
        try:
            self.logger.info(f"Fetching stock info for {ticker}")
            
            # Make API request to Alpha Vantage
            params = {
                "function": "OVERVIEW",
                "symbol": ticker,
                "apikey": self.api_key
            }
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                raise ValueError(f"Alpha Vantage API error: {data['Error Message']}")
            
            # Extract relevant information
            result = {
                "ticker": ticker,
                "name": data.get("Name", ticker),
                "exchange": data.get("Exchange", "Unknown"),
                "sector": data.get("Sector"),
                "industry": data.get("Industry"),
                "currency": data.get("Currency"),
                "market_cap": data.get("MarketCapitalization"),
                "country": data.get("Country"),
                "description": data.get("Description")
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching stock info for {ticker}: {str(e)}")
            raise