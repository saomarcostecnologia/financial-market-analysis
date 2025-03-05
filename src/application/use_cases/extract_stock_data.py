# src/application/use_cases/extract_stock_data.py
from datetime import datetime
from typing import List, Dict, Any, Optional

from src.domain.entities.stock import Stock, StockPrice
from src.domain.interfaces.repositories import StockRepository
from src.domain.interfaces.services import FinancialDataService, ObservabilityService


class ExtractStockDataUseCase:
    """Use case for extracting stock data from external sources."""
    
    def __init__(
        self, 
        financial_data_service: FinancialDataService,
        stock_repository: StockRepository,
        observability_service: ObservabilityService
    ):
        self.financial_data_service = financial_data_service
        self.stock_repository = stock_repository
        self.observability_service = observability_service
    
    def extract_historical_prices(self, ticker: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Extract historical price data for a specific stock."""
        try:
            self.observability_service.log_event(
                "extract_historical_prices_started",
                {"ticker": ticker, "start_date": start_date.isoformat(), "end_date": end_date.isoformat()}
            )
            
            # Get stock info and create Stock entity if it doesn't exist
            stock = self.stock_repository.get_stock(ticker)
            if not stock:
                stock_info = self.financial_data_service.get_stock_info(ticker)
                stock = Stock(
                    ticker=ticker,
                    name=stock_info.get("name", ticker),
                    exchange=stock_info.get("exchange", "Unknown"),
                    sector=stock_info.get("sector"),
                    industry=stock_info.get("industry")
                )
                self.stock_repository.save_stock(stock)
            
            # Get historical prices
            start_time = datetime.now()
            prices = self.financial_data_service.get_stock_historical_prices(ticker, start_date, end_date)
            end_time = datetime.now()
            
            # Save historical prices
            self.stock_repository.save_prices(ticker, prices)
            
            # Log metrics
            extraction_time = (end_time - start_time).total_seconds()
            self.observability_service.track_metric(
                "extraction_time_seconds", 
                extraction_time,
                {"ticker": ticker, "data_type": "historical_prices"}
            )
            self.observability_service.track_metric(
                "records_extracted", 
                len(prices),
                {"ticker": ticker, "data_type": "historical_prices"}
            )
            
            self.observability_service.log_event(
                "extract_historical_prices_completed",
                {
                    "ticker": ticker, 
                    "start_date": start_date.isoformat(), 
                    "end_date": end_date.isoformat(),
                    "records_count": len(prices),
                    "extraction_time_seconds": extraction_time
                }
            )
            
            return prices
            
        except Exception as e:
            self.observability_service.log_event(
                "extract_historical_prices_failed",
                {
                    "ticker": ticker, 
                    "start_date": start_date.isoformat(), 
                    "end_date": end_date.isoformat(),
                    "error": str(e)
                }
            )
            raise


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


# src/application/use_cases/load_stock_data.py
from datetime import datetime
from typing import List, Dict, Any

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