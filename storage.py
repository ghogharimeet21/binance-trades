import numpy as np
from typing import Dict, List
from models import Quote


class MetaData:
    def __init__(self):
        # {symbol: interval: {time: Quote}}
        self.quote_data: Dict[str, Dict[str, Dict[str, Quote]]] = {}




    

    def add_quote(self, quote: Quote):
        if quote.symbol not in self.quote_data:
            self.quote_data[quote.symbol] = {}
        if quote.interval not in self.quote_data[quote.symbol]:
            self.quote_data[quote.symbol][quote.interval] = {}
        if quote.quote_time not in self.quote_data[quote.symbol][quote.interval]:
            self.quote_data[quote.symbol][quote.interval][quote.quote_time] = quote




meta_data = MetaData()