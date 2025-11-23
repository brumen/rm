# class that tracks the markets

import datetime

from logging import getLogger
from collections import OrderedDict
from typing import Tuple, Dict, Optional


_logger = getLogger(__name__)

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
        # keeping track of times of update, insertion.
        self._markets_times: Dict[str, str] = {}

    def new_market(self, market_name: str, market: MARKET_TYPE):
        self._markets.update({market_name: market})
        self._markets_times[market_name] = datetime.datetime.now()

    def get_market_name(self, elt_nb: int = 0) -> Optional[str]:
        "Returns the name of the "

        if not self._markets:
            return None

        market_names = list(self._markets.keys())

        return market_names[elt_nb]

    def get_latest(self, elt_nb: int = -1) -> Optional[MARKET_TYPE]:
        "Chooses the element in the ordered list. By default, last one."

        if not self._markets:
            return None

        values_l = list(self._markets.values())  # TODO: THIS IS INEFFICIENT

        return values_l[elt_nb]

    # def insert_rotate(self, new_market_name: str, new_market: MARKET_TYPE):
    #     # inserts the new market at the end, and removes the old
    #     #   market from the front.
    #     insertion_time = datetime.datetime.now()

    #     self._markets.pop(0)  # remove the first market
    #     # reinsert all the other elements TODO: HIGHLY INEFFICIENT
    #     new_markets = OrderedDict()
    #     for market_name, market in self._markets.items():
    #         new_market
    #     self._markets.insert(new_market_name, (insertion_time, new_market))

    def __repr__(self):
        class_name = self.__class__

        return f'{class_name}: {self._markets.__repr__()}'

    def insert_preserve_names(self, new_market, market_name: str = 'Current'):
        """ Inserts the new market by preserving the existing names, where the
            original markets are shifted down.

            E.g. if we have

            Current  Market_1  Market_2
            M0         M1        M2

            then inserting the market produces

            Current  Market_1  Market_2
            M1         M2        new_market

            In case there is no new markets, then use market_name

        :param new_market: market to insert into the MarketTracker
        :param market_name: market name used in case there are no
            other markets.
        """

        _logger.info(
            f'Inserting new market: {market_name}'
        )

        self._markets[market_name] = new_market

        _logger.info(f'ALL_MARKETS = {self._simplified_markets()}')
        # TODO: THIS BELOW IS WRONG!!! FIX!
        return

        # markets are existing, do the moving
        new_markets = OrderedDict()
        if self._markets.items():
            for old_idx, (old_name, old_mkt) in enumerate(self._markets.items()):
                if old_idx == 0:
                    current_name = old_name
                    continue  # removing this market

                # we are on old_idx == 1...
                new_markets[current_name] = old_mkt  # this is the switch
                current_name = old_name

            # finally we add the new_market
            new_markets[current_name] = new_market
        else:
            new_markets[market_name] = new_market

        self._markets = new_markets

        _logger.info(
            f'After insertion ALL_MARKETS: {self._markets.keys()}'
        )

    def _simplified_markets(self):
        return {
            market_name: len(market) for market_name, market in self.items()
        }

    def __getitem__(self, market_name) -> Optional[MARKET_TYPE]:

        if isinstance(market_name, str):  # calling by market name
            potential_market = self._markets.get(market_name)
            if potential_market is None:
                _logger.warn(
                    f"Could not find {market_name} in "
                    f"ALL_MARKETS: {self._simplified_markets()}"
                )

            return potential_market

        if isinstance(market_name, int):
            market_under_nb = self.get_latest(market_name)
            if market_under_nb is None:
                _logger.warn(
                    f"Could not find market nb {market_name} "
                    f"in ALL_MARKETS: {self._simplified_markets()}"
                )

            return market_under_nb

        _logger.warn(
            f'Could not recognize market name: {market_name}'
        )

        return None

    def __setitem__(self, market_name, market):
        self._markets[market_name] = market

    def __iter__(self):
        for key, value in self._markets.items():
            yield key, value

    def items(self):
        return iter(self)
