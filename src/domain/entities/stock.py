# src/domain/entities/stock.py
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass(frozen=True)
class StockPrice:
    """Value object representing a stock price at a specific point in time."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: Optional[float] = None


@dataclass
class Stock:
    """Entity representing a stock."""
    ticker: str
    name: str
    exchange: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    prices: List[StockPrice] = None

    def __post_init__(self):
        if self.prices is None:
            self.prices = []

    def add_price(self, price: StockPrice) -> None:
        """Add a new price point to the stock's price history."""
        self.prices.append(price)

    def get_latest_price(self) -> Optional[StockPrice]:
        """Get the most recent price point."""
        if not self.prices:
            return None
        return max(self.prices, key=lambda price: price.timestamp)


# src/domain/entities/market_data.py
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any


@dataclass
class MarketData:
    """Entity representing market data from various sources."""
    source_id: str
    data_type: str
    timestamp: datetime
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None