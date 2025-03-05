import boto3
from src.domain.interfaces.repositories import StockRepository

class DynamoRepository(StockRepository):
    def __init__(self, table_name):
        self.table_name = table_name
        self.dynamo_client = boto3.resource('dynamodb')
        self.table = self.dynamo_client.Table(table_name)
    
    def save(self, stock):
        # Implementação para salvar no DynamoDB
        pass
    
    def find_by_symbol(self, symbol):
        # Implementação para buscar no DynamoDB
        pass
