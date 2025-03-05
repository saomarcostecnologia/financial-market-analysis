from src.application.use_cases.extract_stock_data import ExtractStockDataUseCase
from src.application.use_cases.transform_stock_data import TransformStockDataUseCase
from src.application.use_cases.load_stock_data import LoadStockDataUseCase
from src.interfaces.factories.repository_factory import RepositoryFactory

def run_batch_job(symbols, start_date, end_date):
    # Implementação do job batch
    market_data_service = RepositoryFactory.create_market_data_service()
    repository = RepositoryFactory.create_stock_repository()
    
    extract_use_case = ExtractStockDataUseCase(market_data_service)
    transform_use_case = TransformStockDataUseCase()
    load_use_case = LoadStockDataUseCase(repository)
    
    for symbol in symbols:
        raw_data = extract_use_case.execute(symbol, start_date, end_date)
        transformed_data = transform_use_case.execute(raw_data)
        loaded_count = load_use_case.execute(transformed_data)
        print(f"Processed {loaded_count} records for {symbol}")
