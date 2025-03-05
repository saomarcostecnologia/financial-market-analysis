# src/infrastructure/repositories/s3_repository.py
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import io

import boto3
import pandas as pd

from src.domain.entities.stock import Stock, StockPrice
from src.domain.entities.market_data import MarketData
from src.domain.interfaces.repositories import StockRepository, MarketDataRepository
from src.infrastructure.config.settings import Settings


class S3StockRepository(StockRepository):
    """Implementation of StockRepository using S3."""
    
    def __init__(self, settings: Settings, s3_client=None):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.s3_client = s3_client or boto3.client('s3')
        self.bucket_name = settings.S3_DATA_BUCKET
    
    def save_stock(self, stock: Stock) -> None:
        """Save stock metadata to S3."""
        try:
            key = f"stocks/{stock.ticker}/metadata.json"
            
            # Convert to dict for JSON serialization
            stock_dict = {
                "ticker": stock.ticker,
                "name": stock.name,
                "exchange": stock.exchange,
                "sector": stock.sector,
                "industry": stock.industry,
                "last_updated": datetime.now().isoformat()
            }
            
            # Save to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(stock_dict, indent=2),
                ContentType="application/json"
            )
            
            self.logger.info(f"Saved metadata for stock {stock.ticker} to S3")
            
        except Exception as e:
            self.logger.error(f"Error saving stock {stock.ticker} to S3: {str(e)}")
            raise
    
    def get_stock(self, ticker: str) -> Optional[Stock]:
        """Get stock metadata from S3."""
        try:
            key = f"stocks/{ticker}/metadata.json"
            
            try:
                # Try to get object from S3
                response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                stock_dict = json.loads(response['Body'].read().decode('utf-8'))
                
                # Create Stock entity
                stock = Stock(
                    ticker=stock_dict["ticker"],
                    name=stock_dict["name"],
                    exchange=stock_dict["exchange"],
                    sector=stock_dict.get("sector"),
                    industry=stock_dict.get("industry")
                )
                
                return stock
                
            except self.s3_client.exceptions.NoSuchKey:
                # Stock metadata doesn't exist
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting stock {ticker} from S3: {str(e)}")
            raise
    
    def save_prices(self, ticker: str, prices: List[StockPrice]) -> None:
        """Save historical prices for a stock to S3 in Parquet format."""
        try:
            if not prices:
                self.logger.warning(f"No prices to save for {ticker}")
                return
            
            # Convert to DataFrame for Parquet serialization
            data = []
            for price in prices:
                data.append({
                    "timestamp": price.timestamp,
                    "open": price.open,
                    "high": price.high,
                    "low": price.low,
                    "close": price.close,
                    "volume": price.volume,
                    "adjusted_close": price.adjusted_close
                })
            
            df = pd.DataFrame(data)
            
            # Determine time range for file naming
            min_date = min(prices, key=lambda p: p.timestamp).timestamp.date()
            max_date = max(prices, key=lambda p: p.timestamp).timestamp.date()
            
            # Create key with partitioning
            key = f"stocks/{ticker}/prices/year={min_date.year}/month={min_date.month:02d}/prices_{min_date}_{max_date}.parquet"
            
            # Convert DataFrame to Parquet and save to S3
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue()
            )
            
            self.logger.info(f"Saved {len(prices)} price points for {ticker} to S3 from {min_date} to {max_date}")
            
        except Exception as e:
            self.logger.error(f"Error saving prices for {ticker} to S3: {str(e)}")
            raise
    
    def get_prices(self, ticker: str, start_date: datetime, end_date: datetime) -> List[StockPrice]:
        """Get historical prices for a stock from S3."""
        try:
            # List all price files for the stock in the date range
            start_year = start_date.year
            end_year = end_date.year
            
            all_prices = []
            
            for year in range(start_year, end_year + 1):
                # Calculate month range for this year
                start_month = 1 if year > start_year else start_date.month
                end_month = 12 if year < end_year else end_date.month
                
                for month in range(start_month, end_month + 1):
                    prefix = f"stocks/{ticker}/prices/year={year}/month={month:02d}/"
                    
                    # List files in this partition
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.bucket_name,
                        Prefix=prefix
                    )
                    
                    if 'Contents' not in response:
                        continue
                    
                    # Process each file
                    for obj in response['Contents']:
                        file_key = obj['Key']
                        
                        # Get file content
                        file_response = self.s3_client.get_object(
                            Bucket=self.bucket_name,
                            Key=file_key
                        )
                        
                        # Read Parquet file into DataFrame
                        buffer = io.BytesIO(file_response['Body'].read())
                        df = pd.read_parquet(buffer)
                        
                        # Filter by date range
                        df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
                        
                        # Convert to StockPrice entities
                        for _, row in df.iterrows():
                            price = StockPrice(
                                timestamp=row['timestamp'],
                                open=row['open'],
                                high=row['high'],
                                low=row['low'],
                                close=row['close'],
                                volume=row['volume'],
                                adjusted_close=row['adjusted_close']
                            )
                            all_prices.append(price)
            
            # Sort by timestamp
            all_prices.sort(key=lambda p: p.timestamp)
            
            self.logger.info(f"Retrieved {len(all_prices)} price points for {ticker} from {start_date} to {end_date}")
            return all_prices
            
        except Exception as e:
            self.logger.error(f"Error getting prices for {ticker} from S3: {str(e)}")
            raise


class S3MarketDataRepository(MarketDataRepository):
    """Implementation of MarketDataRepository using S3."""
    
    def __init__(self, settings: Settings, s3_client=None):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.s3_client = s3_client or boto3.client('s3')
        self.bucket_name = settings.S3_DATA_BUCKET
    
    def save_data(self, data: MarketData) -> None:
        """Save market data to S3."""
        try:
            # Create a partition structure
            timestamp = data.timestamp
            key = f"market_data/{data.source_id}/{data.data_type}/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/{timestamp.timestamp()}.json"
            
            # Convert to dict for JSON serialization
            data_dict = {
                "source_id": data.source_id,
                "data_type": data.data_type,
                "timestamp": data.timestamp.isoformat(),
                "data": data.data,
                "metadata": data.metadata
            }
            
            # Save to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data_dict, indent=2),
                ContentType="application/json"
            )
            
            self.logger.info(f"Saved market data from {data.source_id} of type {data.data_type} to S3")
            
        except Exception as e:
            self.logger.error(f"Error saving market data to S3: {str(e)}")
            raise
    
    def get_data(self, source_id: str, data_type: str, start_date: datetime, end_date: datetime) -> List[MarketData]:
        """Get market data from S3 within a date range."""
        try:
            all_data = []
            
            # List all days in the date range
            current_date = start_date.date()
            end_date_day = end_date.date()
            
            while current_date <= end_date_day:
                # Create prefix for this day
                prefix = f"market_data/{source_id}/{data_type}/year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/"
                
                # List files in this partition
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix
                )
                
                if 'Contents' in response:
                    # Process each file
                    for obj in response['Contents']:
                        file_key = obj['Key']
                        
                        # Get file content
                        file_response = self.s3_client.get_object(
                            Bucket=self.bucket_name,
                            Key=file_key
                        )
                        
                        # Parse JSON
                        data_dict = json.loads(file_response['Body'].read().decode('utf-8'))
                        
                        # Create MarketData entity
                        timestamp = datetime.fromisoformat(data_dict["timestamp"])
                        
                        # Filter by exact time range
                        if start_date <= timestamp <= end_date:
                            market_data = MarketData(
                                source_id=data_dict["source_id"],
                                data_type=data_dict["data_type"],
                                timestamp=timestamp,
                                data=data_dict["data"],
                                metadata=data_dict.get("metadata")
                            )
                            all_data.append(market_data)
                
                # Move to next day
                current_date = current_date.replace(day=current_date.day + 1)
            
            # Sort by timestamp
            all_data.sort(key=lambda d: d.timestamp)
            
            self.logger.info(f"Retrieved {len(all_data)} market data items from {source_id} of type {data_type} from {start_date} to {end_date}")
            return all_data
            
        except Exception as e:
            self.logger.error(f"Error getting market data from S3: {str(e)}")
            raise


# src/infrastructure/repositories/dynamo_repository.py
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
import decimal

import boto3
from boto3.dynamodb.conditions import Key, Attr

from src.domain.entities.stock import Stock, StockPrice
from src.domain.entities.market_data import MarketData
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
        self.dynamodb = dynamodb_resource or boto3.resource('dynamodb')
        self.stocks_table = self.dynamodb.Table(settings.DYNAMODB_STOCKS_TABLE)
        self.prices_table = self.dynamodb.Table(settings.DYNAMODB_PRICES_TABLE)
    
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