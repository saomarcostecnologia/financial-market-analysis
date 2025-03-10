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
    
    def start_trace(self, trace_name: str, trace_data: Dict[str, Any] = None) -> str:
        """Start a new trace and return the trace ID."""
        pass
    
    def end_trace(self, trace_id: str, success: bool = True, result_data: Dict[str, Any] = None) -> None:
        """End a trace with the given trace ID."""
        pass


class DataMaskingService(ABC):
    """Interface for data masking service."""
    
    @abstractmethod
    def mask_sensitive_data(self, data: Dict[str, Any], fields_to_mask: List[str]) -> Dict[str, Any]:
        """Mask sensitive data fields."""
        pass