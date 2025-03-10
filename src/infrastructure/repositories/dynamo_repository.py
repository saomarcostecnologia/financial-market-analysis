# src/infrastructure/repositories/dynamo_repository.py
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr

from src.domain.entities.stock import Stock, StockPrice
from src.domain.interfaces.repositories import StockRepository
from src.infrastructure.config.settings import Settings


# Helper class to convert DynamoDB Decimal to float
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o) if o % 1 > 0 else int(o)
        return super(DecimalEncoder, self).default(o)


class DynamoDBStockRepository(StockRepository):
    """Implementation of StockRepository using DynamoDB."""
    
    def __init__(self, settings: Settings, dynamodb_resource=None):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.dynamodb = dynamodb_resource or boto3.resource('dynamodb', region_name=settings.AWS_REGION)
        
        # Nomes das tabelas
        self.stocks_table_name = settings.DYNAMODB_STOCKS_TABLE
        self.prices_table_name = settings.DYNAMODB_PRICES_TABLE
        
        # Inicializa as tabelas (lazy loading)
        self._stocks_table = None
        self._prices_table = None
        
        # Verifica se as tabelas existem
        self._ensure_tables_exist()
    
    @property
    def stocks_table(self):
        """Get stocks table, creating it if it doesn't exist."""
        if self._stocks_table is None:
            try:
                self._stocks_table = self.dynamodb.Table(self.stocks_table_name)
                # Check if table exists by making a simple request
                self._stocks_table.table_status
            except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
                self._create_stocks_table()
                self._stocks_table = self.dynamodb.Table(self.stocks_table_name)
        return self._stocks_table
    
    @property
    def prices_table(self):
        """Get prices table, creating it if it doesn't exist."""
        if self._prices_table is None:
            try:
                self._prices_table = self.dynamodb.Table(self.prices_table_name)
                # Check if table exists by making a simple request
                self._prices_table.table_status
            except self.dynamodb.meta.client.exceptions.ResourceNotFoundException:
                self._create_prices_table()
                self._prices_table = self.dynamodb.Table(self.prices_table_name)
        return self._prices_table
    
    def _ensure_tables_exist(self):
        """Check if required tables exist, create them if they don't."""
        existing_tables = self.dynamodb.meta.client.list_tables()['TableNames']
        
        if self.stocks_table_name not in existing_tables:
            self._create_stocks_table()
        
        if self.prices_table_name not in existing_tables:
            self._create_prices_table()
    
    def _create_stocks_table(self):
        """Create the stocks table in DynamoDB."""
        self.logger.info(f"Creating DynamoDB table: {self.stocks_table_name}")
        
        table = self.dynamodb.create_table(
            TableName=self.stocks_table_name,
            KeySchema=[
                {
                    'AttributeName': 'ticker',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'ticker',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        # Wait for the table to be created
        table.meta.client.get_waiter('table_exists').wait(TableName=self.stocks_table_name)
        self.logger.info(f"Table {self.stocks_table_name} created successfully")
    
    def _create_prices_table(self):
        """Create the prices table in DynamoDB."""
        self.logger.info(f"Creating DynamoDB table: {self.prices_table_name}")
        
        table = self.dynamodb.create_table(
            TableName=self.prices_table_name,
            KeySchema=[
                {
                    'AttributeName': 'ticker',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'ticker',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        
        # Wait for the table to be created
        table.meta.client.get_waiter('table_exists').wait(TableName=self.prices_table_name)
        self.logger.info(f"Table {self.prices_table_name} created successfully")
    
    def save_stock(self, stock: Stock) -> None:
        """Save a stock to DynamoDB."""
        try:
            # Create item
            item = {
                "ticker": stock.ticker,
                "name": stock.name,
                "exchange": stock.exchange,
                "updatedAt": datetime.now().isoformat()
            }
            
            if stock.sector:
                item["sector"] = stock.sector
            
            if stock.industry:
                item["industry"] = stock.industry
            
            # Save to DynamoDB
            self.stocks_table.put_item(Item=item)
            
            self.logger.info(f"Saved stock {stock.ticker} to DynamoDB")
            
        except Exception as e:
            self.logger.error(f"Error saving stock {stock.ticker} to DynamoDB: {str(e)}")
            raise
    
    def get_stock(self, ticker: str) -> Optional[Stock]:
        """Get a stock from DynamoDB."""
        try:
            response = self.stocks_table.get_item(Key={"ticker": ticker})
            
            if "Item" in response:
                item = response["Item"]
                
                stock = Stock(
                    ticker=item["ticker"],
                    name=item["name"],
                    exchange=item["exchange"],
                    sector=item.get("sector"),
                    industry=item.get("industry")
                )
                
                return stock
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting stock {ticker} from DynamoDB: {str(e)}")
            raise
    
    def save_prices(self, ticker: str, prices: List[StockPrice]) -> None:
        """Save historical prices for a stock to DynamoDB."""
        try:
            if not prices:
                self.logger.warning(f"No prices to save for {ticker}")
                return
            
            # Use batch write for efficiency
            with self.prices_table.batch_writer() as batch:
                for price in prices:
                    item = {
                        "ticker": ticker,
                        "timestamp": price.timestamp.isoformat(),
                        "open": decimal.Decimal(str(price.open)),
                        "high": decimal.Decimal(str(price.high)),
                        "low": decimal.Decimal(str(price.low)),
                        "close": decimal.Decimal(str(price.close)),
                        "volume": price.volume
                    }
                    
                    if price.adjusted_close is not None:
                        item["adjusted_close"] = decimal.Decimal(str(price.adjusted_close))
                    
                    batch.put_item(Item=item)
            
            self.logger.info(f"Saved {len(prices)} price points for {ticker} to DynamoDB")
            
        except Exception as e:
            self.logger.error(f"Error saving prices for {ticker} to DynamoDB: {str(e)}")
            raise
    
    def get_prices(self, ticker: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Get historical prices for a stock from DynamoDB."""
        try:
            # Convert dates to ISO format for string comparison
            start_iso = start_date.isoformat()
            end_iso = end_date.isoformat()
            
            # Query using GSI for ticker and timestamp range
            response = self.prices_table.query(
                KeyConditionExpression=Key('ticker').eq(ticker) & 
                                      Key('timestamp').between(start_iso, end_iso)
            )
            
            prices = []
            for item in response.get('Items', []):
                price = StockPrice(
                    timestamp=datetime.fromisoformat(item["timestamp"]),
                    open=float(item["open"]),
                    high=float(item["high"]),
                    low=float(item["low"]),
                    close=float(item["close"]),
                    volume=item["volume"],
                    adjusted_close=float(item["adjusted_close"]) if "adjusted_close" in item else None
                )
                prices.append(price)
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = self.prices_table.query(
                    KeyConditionExpression=Key('ticker').eq(ticker) & 
                                          Key('timestamp').between(start_iso, end_iso),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                
                for item in response.get('Items', []):
                    price = StockPrice(
                        timestamp=datetime.fromisoformat(item["timestamp"]),
                        open=float(item["open"]),
                        high=float(item["high"]),
                        low=float(item["low"]),
                        close=float(item["close"]),
                        volume=item["volume"],
                        adjusted_close=float(item["adjusted_close"]) if "adjusted_close" in item else None
                    )
                    prices.append(price)
            
            # Sort by timestamp
            prices.sort(key=lambda p: p.timestamp)
            
            self.logger.info(f"Retrieved {len(prices)} price points for {ticker} from DynamoDB")
            return prices
            
        except Exception as e:
            self.logger.error(f"Error getting prices for {ticker} from DynamoDB: {str(e)}")
            raise


class DynamoRepository:
    """Legacy class for backward compatibility."""
    def __init__(self, table_name, region_name='us-east-1'):
        self.logger = logging.getLogger(__name__)
        self.logger.warning("DynamoRepository is deprecated, use DynamoDBStockRepository instead")
        settings = Settings()
        self.stock_repository = DynamoDBStockRepository(settings)