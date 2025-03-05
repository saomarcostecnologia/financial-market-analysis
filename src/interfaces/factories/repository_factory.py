from src.infrastructure.repositories.s3_repository import S3Repository
from src.infrastructure.repositories.dynamo_repository import DynamoRepository
from src.infrastructure.adapters.yahoo_finance_adapter import YahooFinanceAdapter
from src.infrastructure.adapters.alpha_vantage_adapter import AlphaVantageAdapter

class RepositoryFactory:
    @staticmethod
    def create_stock_repository(repo_type='dynamo'):
        if repo_type == 's3':
            return S3Repository('financial-market-bucket')
        else:
            return DynamoRepository('financial_market_table')
    
    @staticmethod
    def create_market_data_service(service_type='yahoo'):
        if service_type == 'alpha':
            return AlphaVantageAdapter()
        else:
            return YahooFinanceAdapter()
