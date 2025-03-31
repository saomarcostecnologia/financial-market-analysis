# src/application/use_cases/batch_process_stocks.py
import logging
import concurrent.futures
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from src.domain.interfaces.repositories import StockRepository, MarketDataRepository
from src.domain.interfaces.services import FinancialDataService, DataProcessingService, ObservabilityService
from src.application.use_cases.extract_stock_data import ExtractStockDataUseCase
from src.application.use_cases.transform_stock_data import TransformStockDataUseCase
from src.application.use_cases.load_stock_data import LoadStockDataUseCase


class BatchProcessStocksUseCase:
    """Use case for batch processing multiple stock tickers."""
    
    def __init__(
        self,
        financial_data_service: FinancialDataService,
        data_processing_service: DataProcessingService,
        stock_repository: StockRepository,
        market_data_repository: MarketDataRepository,
        observability_service: ObservabilityService,
        max_workers: int = 5
    ):
        self.logger = logging.getLogger(__name__)
        self.max_workers = max_workers
        
        # Initialize component use cases
        self.extract_use_case = ExtractStockDataUseCase(
            financial_data_service,
            stock_repository,
            observability_service
        )
        
        self.transform_use_case = TransformStockDataUseCase(
            data_processing_service,
            stock_repository,
            observability_service
        )
        
        self.load_use_case = LoadStockDataUseCase(
            market_data_repository,
            observability_service
        )
        
        self.observability_service = observability_service
    
    def process_tickers(
        self, 
        tickers: List[str], 
        start_date: datetime, 
        end_date: datetime,
        calculate_indicators: bool = True,
        parallel: bool = True
    ) -> Dict[str, Any]:
        """Process multiple stock tickers in batch."""
        self.logger.info(f"Starting batch processing of {len(tickers)} tickers from {start_date} to {end_date}")
        
        # Generate a batch ID for tracking
        batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        trace_id = self.observability_service.start_trace("batch_process", {
            "batch_id": batch_id,
            "tickers": tickers,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        })
        
        results = {}
        errors = []
        
        try:
            if parallel and len(tickers) > 1:
                # Process tickers in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Map tickers to futures
                    future_to_ticker = {
                        executor.submit(self._process_single_ticker, ticker, start_date, end_date, calculate_indicators): ticker
                        for ticker in tickers
                    }
                    
                    # Process results as they complete
                    for future in concurrent.futures.as_completed(future_to_ticker):
                        ticker = future_to_ticker[future]
                        try:
                            result = future.result()
                            results[ticker] = result
                        except Exception as e:
                            self.logger.error(f"Error processing {ticker}: {str(e)}")
                            results[ticker] = {"status": "error", "error": str(e)}
                            errors.append({"ticker": ticker, "error": str(e)})
            else:
                # Process tickers sequentially
                for ticker in tickers:
                    try:
                        result = self._process_single_ticker(ticker, start_date, end_date, calculate_indicators)
                        results[ticker] = result
                    except Exception as e:
                        self.logger.error(f"Error processing {ticker}: {str(e)}")
                        results[ticker] = {"status": "error", "error": str(e)}
                        errors.append({"ticker": ticker, "error": str(e)})
            
            # Calculate summary statistics
            success_count = sum(1 for r in results.values() if r.get("status") == "success")
            
            # Complete the trace
            self.observability_service.end_trace(trace_id, success=len(errors) == 0, result_data={
                "total_tickers": len(tickers),
                "successful": success_count,
                "failed": len(tickers) - success_count,
                "errors": errors[:5] if errors else None  # Include first 5 errors at most
            })
            
            return {
                "batch_id": batch_id,
                "total": len(tickers),
                "successful": success_count,
                "failed": len(tickers) - success_count,
                "results": results
            }
            
        except Exception as e:
            self.logger.error(f"Error in batch processing: {str(e)}")
            self.observability_service.end_trace(trace_id, success=False, result_data={"error": str(e)})
            raise
    
    def _process_single_ticker(
        self, 
        ticker: str, 
        start_date: datetime, 
        end_date: datetime,
        calculate_indicators: bool
    ) -> Dict[str, Any]:
        """Process a single ticker through the ETL pipeline."""
        ticker_trace_id = self.observability_service.start_trace("ticker_process", {
            "ticker": ticker,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        })
        
        try:
            # Extract
            self.logger.info(f"Extracting data for {ticker}")
            prices = self.extract_use_case.extract_historical_prices(ticker, start_date, end_date)
            
            result = {
                "ticker": ticker,
                "status": "success",
                "price_points": len(prices),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
            
            # Transform (calculate indicators)
            if calculate_indicators and prices:
                self.logger.info(f"Calculating indicators for {ticker}")
                indicators = self.transform_use_case.calculate_technical_indicators(ticker, start_date, end_date)
                
                # Store the number of indicators calculated
                result["indicators_calculated"] = len(indicators)
                
                # Load the processed data
                self.logger.info(f"Loading processed data for {ticker}")
                self.load_use_case.load_processed_data(ticker, "technical_indicators", indicators)
            
            # Complete the trace
            self.observability_service.end_trace(ticker_trace_id, success=True, result_data=result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing ticker {ticker}: {str(e)}")
            self.observability_service.end_trace(ticker_trace_id, success=False, result_data={"error": str(e)})
            raise