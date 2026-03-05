from typing import List
from models import Quote



def resample(timeframe: int, quotes: List[Quote]):

    # find open time
    open_time = 0
    for quote in quotes:
        if not open_time:
            open_time = quote.quote_time
        elif quote.quote_time < open_time:
            open_time = quote.quote_time
    

    open_price = [quote.quote_time for quote in quotes if quote.quote_time == open_time][0].open_price
    high_price = max([quote.high_price for quote in quotes])
    low_price = min([quote.low_price for quote in quotes])
    close_price = [quote.quote_time for quote in quotes if quote.quote_time == open_time + timeframe][0].close_price