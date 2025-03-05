class MarketData:
    def __init__(self, stock_symbol, timestamp, open_price, high, low, close, volume):
        self.stock_symbol = stock_symbol
        self.timestamp = timestamp
        self.open_price = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
