# src/domain/entities/market_data.py
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional


@dataclass
class MarketData:
    """Entity representing market data from various sources."""
    source_id: str
    data_type: str
    timestamp: datetime
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

    def __init__(self, source_id: str, data_type: str, timestamp: datetime, data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None):
        self.source_id = source_id
        self.data_type = data_type
        self.timestamp = timestamp
        self.data = data
        self.metadata = metadata