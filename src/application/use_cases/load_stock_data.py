# src/application/use_cases/load_stock_data.py
from datetime import datetime
from typing import List, Optional, Dict, Any

from src.domain.entities.stock import Stock, StockPrice
from src.domain.entities.market_data import MarketData
from src.domain.interfaces.repositories import MarketDataRepository
from src.domain.interfaces.services import ObservabilityService, DataMaskingService


class LoadStockDataUseCase:
    """Use case for loading processed stock data into storage."""
    
    def __init__(
        self,
        market_data_repository: MarketDataRepository,
        observability_service: ObservabilityService,
        data_masking_service: DataMaskingService = None
    ):
        self.market_data_repository = market_data_repository
        self.observability_service = observability_service
        self.data_masking_service = data_masking_service
    
    def load_processed_data(self, source_id: str, data_type: str, data: Dict[str, Any], sensitive_fields: List[str] = None) -> None:
        """Load processed data into storage, applying masking if needed."""
        try:
            self.observability_service.log_event(
                "load_processed_data_started",
                {"source_id": source_id, "data_type": data_type}
            )
            
            # Apply data masking if needed
            if sensitive_fields and self.data_masking_service:
                data = self.data_masking_service.mask_sensitive_data(data, sensitive_fields)
            
            # Create MarketData entity
            market_data = MarketData(
                source_id=source_id,
                data_type=data_type,
                timestamp=datetime.now(),
                data=data,
                metadata={
                    "processed_at": datetime.now().isoformat(),
                    "version": "1.0"
                }
            )
            
            # Save to repository
            start_time = datetime.now()
            self.market_data_repository.save_data(market_data)
            end_time = datetime.now()
            
            # Log metrics
            load_time = (end_time - start_time).total_seconds()
            self.observability_service.track_metric(
                "load_time_seconds", 
                load_time,
                {"source_id": source_id, "data_type": data_type}
            )
            
            self.observability_service.log_event(
                "load_processed_data_completed",
                {
                    "source_id": source_id, 
                    "data_type": data_type,
                    "load_time_seconds": load_time
                }
            )
            
        except Exception as e:
            self.observability_service.log_event(
                "load_processed_data_failed",
                {
                    "source_id": source_id, 
                    "data_type": data_type,
                    "error": str(e)
                }
            )
            raise