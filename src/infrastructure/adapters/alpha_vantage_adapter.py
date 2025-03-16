import requests
from src.domain.interfaces.services import MarketDataService
from src.infrastructure.config.settings import Settings

class AlphaVantageAdapter(MarketDataService):
    def __init__(self, settings: Settings, ssm_client=None):
        self.settings = settings
        self.logger = logging.getLogger(__name__)
        self.ssm_client = ssm_client or boto3.client('ssm', region_name=settings.AWS_REGION)
        self.api_key = self._get_api_key()
        self.base_url = "https://www.alphavantage.co/query"
    
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
