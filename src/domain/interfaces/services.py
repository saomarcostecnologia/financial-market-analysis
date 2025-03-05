from abc import ABC, abstractmethod

class MarketDataService(ABC):
    @abstractmethod
    def get_historical_data(self, symbol, start_date, end_date):
        pass
