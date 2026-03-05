









class Quote:
    def __init__(
        self,
        symbol,
        interval,
        quote_open_time,
        quote_date,
        quote_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        closed,
        quote_close_time=None,
    ):
        self.symbol = symbol
        self.interval = interval
        self.quote_open_time = quote_open_time
        self.quote_date = quote_date
        self.quote_time = quote_time
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.closed = closed
        self.quote_close_time = quote_close_time
    


    def __str__(self):
        return f"Quote(symbol={self.symbol}, interval={self.interval}, quote_open_time={self.quote_open_time}, quote_date={self.quote_date}, quote_time={self.quote_time}, open_price={self.open_price}, high_price={self.high_price}, low_price={self.low_price}, close_price={self.close_price}, volume={self.volume}, closed={self.closed}, quote_close_time={self.quote_close_time})"
