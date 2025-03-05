import requests
from src.domain.interfaces.services import MarketDataService
from src.infrastructure.config.settings import Settings

class AlphaVantageAdapter(MarketDataService):
    def __init__(self):
        self.api_key = Settings.ALPHA_VANTAGE_API_KEY
        self.base_url = 'https://www.alphavantage.co/query'
    
    def get_historical_data(self, symbol, start_date, end_date):
        # Obter dados da Alpha Vantage
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'full'
        }
        response = requests.get(self.base_url, params=params)
        return response.json()
