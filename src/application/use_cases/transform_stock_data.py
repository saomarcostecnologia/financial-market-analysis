# src/application/use_cases/transform_stock_data.py
from datetime import datetime
from typing import List, Dict, Any

from src.domain.entities.stock import StockPrice
from src.domain.interfaces.services import DataProcessingService, ObservabilityService
from src.domain.interfaces.repositories import StockRepository


class TransformStockDataUseCase:
    """Use case for transforming raw stock data."""
    
    def __init__(
        self,
        data_processing_service: DataProcessingService,
        stock_repository: StockRepository,
        observability_service: ObservabilityService
    ):
        self.data_processing_service = data_processing_service
        self.stock_repository = stock_repository
        self.observability_service = observability_service
    
    def calculate_technical_indicators(self, ticker: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Calculate technical indicators for a stock's historical prices."""
        try:
            self.observability_service.log_event(
                "calculate_indicators_started",
                {"ticker": ticker, "start_date": start_date.isoformat(), "end_date": end_date.isoformat()}
            )
            
            # Get historical prices
            prices = self.stock_repository.get_prices(ticker, start_date, end_date)
            
            if not prices:
                self.observability_service.log_event(
                    "calculate_indicators_no_data",
                    {"ticker": ticker, "start_date": start_date.isoformat(), "end_date": end_date.isoformat()}
                )
                return {}
            
            # Convert to format expected by processing service
            price_data = [
                {
                    "timestamp": price.timestamp.isoformat(),
                    "open": price.open,
                    "high": price.high,
                    "low": price.low,
                    "close": price.close,
                    "volume": price.volume,
                    "adjusted_close": price.adjusted_close
                }
                for price in prices
            ]
            
            # Process data to calculate indicators
            start_time = datetime.now()
            result = self.data_processing_service.process_batch_data(
                f"{ticker}_{start_date.date()}_{end_date.date()}", 
                price_data
            )
            end_time = datetime.now()
            
            # Log metrics
            processing_time = (end_time - start_time).total_seconds()
            self.observability_service.track_metric(
                "processing_time_seconds", 
                processing_time,
                {"ticker": ticker, "operation": "calculate_indicators"}
            )
            
            self.observability_service.log_event(
                "calculate_indicators_completed",
                {
                    "ticker": ticker, 
                    "start_date": start_date.isoformat(), 
                    "end_date": end_date.isoformat(),
                    "processing_time_seconds": processing_time,
                    "indicators_calculated": list(result.keys())
                }
            )
            
            return result
            
        except Exception as e:
            self.observability_service.log_event(
                "calculate_indicators_failed",
                {
                    "ticker": ticker, 
                    "start_date": start_date.isoformat(), 
                    "end_date": end_date.isoformat(),
                    "error": str(e)
                }
            )
            raise