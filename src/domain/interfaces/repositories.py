# src/domain/interfaces/repositories.py
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional, Dict, Any

from src.domain.entities.stock import Stock, StockPrice
from src.domain.entities.market_data import MarketData


class StockRepository(ABC):
    """Interface for stock data repository."""
    
    @abstractmethod
    def save_stock(self, stock: Stock) -> None:
        """Save a stock to the repository."""
        pass
    
    @abstractmethod
    def get_stock(self, ticker: str) -> Optional[Stock]:
        """Get a stock by its ticker."""
        pass
    
    @abstractmethod
    def save_prices(self, ticker: str, prices: List[StockPrice]) -> None:
        """Save historical prices for a stock."""
        pass
    
    @abstractmethod
    def get_prices(self, ticker: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Get historical prices for a stock within a date range."""
        pass


class MarketDataRepository(ABC):
    """Interface for general market data repository."""
    
    @abstractmethod
    def save_data(self, data: MarketData) -> None:
        """Save market data to the repository."""
        pass
    
    @abstractmethod
    def get_data(self, source_id: str, data_type: str, start_date: datetime, end_date: datetime) -> List[MarketData]:
        """Get market data from a specific source within a date range."""
        pass


# src/domain/interfaces/services.py
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.domain.entities.stock import StockPrice


class FinancialDataService(ABC):
    """Interface for financial data service."""
    
    @abstractmethod
    def get_stock_historical_prices(self, ticker: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Retrieve historical stock prices from an external service."""
        pass
    
    @abstractmethod
    def get_stock_info(self, ticker: str) -> Dict[str, Any]:
        """Retrieve general information about a stock."""
        pass


class DataProcessingService(ABC):
    """Interface for data processing service."""
    
    @abstractmethod
    def process_batch_data(self, batch_id: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process a batch of data."""
        pass
    
    @abstractmethod
    def process_stream_data(self, stream_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process streaming data."""
        pass


class DataMaskingService(ABC):
    """Interface for data masking service."""
    
    @abstractmethod
    def mask_sensitive_data(self, data: Dict[str, Any], fields_to_mask: List[str]) -> Dict[str, Any]:
        """Mask sensitive data fields."""
        pass


class ObservabilityService(ABC):
    """Interface for observability service."""
    
    @abstractmethod
    def log_event(self, event_type: str, event_data: Dict[str, Any]) -> None:
        """Log an event for observability."""
        pass
    
    @abstractmethod
    def track_metric(self, metric_name: str, value: float, dimensions: Dict[str, str] = None) -> None:
        """Track a metric for monitoring."""
        pass