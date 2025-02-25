# class that tracks the markets

import datetime

from collections import OrderedDict
from typing import Tuple, Dict, Optional

# original market type
MARKET_TYPE = Optional[Dict[Tuple[str, datetime.date], float]]
# market representation
MARKET_REPR = OrderedDict[str, Tuple[datetime.datetime, MARKET_TYPE]]


class MarketTracker:
    """ Market indexing:
          The oldest market considered is under index 0
          The latest market is under index -1
          Other markets in between have there own numbers, as well as
          naming - e.g. they could be 'current', 'new', 'new 2', 'newest',...
    """

    def __init__(self):
        # the markets that we will follow.
        self._markets: MARKET_REPR = OrderedDict()

    def new_market(self, market_name: str, market: MARKET_TYPE):
        self._markets.insert((market_name, market))

    def get_latest(self) -> Optional[Tuple[datetime.datetime, MARKET_TYPE]]:
        if not self._markets:
            return None

        last_time, last_market = self._markets.values[-1]
        return last_market

    def insert_rotate(self, new_market_name: str, new_market: MARKET_TYPE):
        # inserts the new market at the end, and removes the old
        #   market from the front.
        insertion_time = datetime.datetime.now()

        self._markets.pop(0)  # remove the first market
        self._markets.insert(new_market_name, (insertion_time, new_market))

    def insert_preserve_names(self, new_market):
        insertion_time = datetime.datetime.now()

        first_name, _ = self._markets.pop(0)  # remove the first market

        # generate the new markets # TODO: THIS IS BAD

        new_markets = OrderedDict()
        for old_idx, (old_name, old_mkt) in enumerate(self._markets.items()):
            if old_idx == 0:
                current_name = old_name
                continue  # removing this market

            # we are on old_idx == 1...
            new_markets[current_name] = old_mkt  # this is the switch
            current_name = old_name

        # finally we add the new_market
        new_markets[current_name] = (insertion_time, new_market)
        self._markets = new_markets

    def __getitem__(self, market_name) -> Optional[MARKET_TYPE]:

        if isinstance(market_name, str):  # calling by market name
            market_possible = self._markets.get(market_name)

            if market_possible is None:
                return None  # TODO: THIS SHOULD POSSIBLY RAISE EXCEPTION HERE

            market_insertion, market = market_possible
            return market

        if isinstance(market_name, int):
            market_vals = self._markets.values
            if market_name >= len(market_vals):
                return None

            return market_vals[market_name]
