"""Publish LETF market updates as ``(LETFMarketTypes, f64)`` tuples.

The Rust market consumer reads each Kafka message as one tuple and inserts it
into the current market.
"""

from typing import List, Tuple, Dict
from numpy import random
from time import sleep

from rm.services.base_producer import BaseProducer


class LETFMarketProducer(BaseProducer):

    def __init__(
        self,
        stocks: List[str],
        server_port_topic: Tuple[str, str, str] = (
            "localhost",
            9092,
            "letf.mkt",
        ),
    ):
        super().__init__(server_port_topic)
        self._stocks: List[str] = stocks

        # Keep the simulated stock prices between market updates.
        self.curr_stocks: Dict[str, float] = {
            stock: 50.0 + stock_idx * 5 for stock_idx, stock in enumerate(self._stocks)
        }  # initial values

    def _value_to_publish(self, sleep_between_publish=11.0):
        """Keeps generating new fictitious market for stocks.

        :param sleep_between_publish: sleep time between individual publishes
        """

        while True:

            # The Rust market handler deserializes each message as the tuple
            # (LETFMarketTypes, f64), with enum variants externally tagged.
            for stock, value in self.curr_stocks.items():
                yield ({"Stock": stock}, value)

            for stock in self._stocks:
                # add some random value to the current stock values
                self.curr_stocks[stock] += random.normal(loc=0.0, scale=1.0)
                self.curr_stocks[stock] = max(self.curr_stocks[stock], 10.0)

            sleep(sleep_between_publish)
