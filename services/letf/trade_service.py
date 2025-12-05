""" Load simulator for LETF trades.
"""

import random
import numpy as np

from logging import getLogger
from typing import Tuple, List
from time import sleep

from rm.base_producer import BaseProducer


class LETFTradeProducer(BaseProducer):

    def __init__(
            self,
            stocks: List[str],
            server_port_topic: Tuple[str, str, str] = (
                'localhost',
                9092,
                'letf.positions',
            ),
            beta=2.,
            mkt_producer=None,
            trade_nb_start=5,
    ):
        """ LETF trade producer.
        :param stocks: stocks relevant to the trade producer.
        :param server_port_topic: kafka server port to produce to.
        :param beta: beta of the letf positions.
        :param market_producer: market producer to use
        """

        super().__init__(server_port_topic)
        self._stocks = stocks
        self._beta = beta
        self._mkt_producer = mkt_producer
        self._trade_nb_start = trade_nb_start

    def _value_1(self, trade_nb: int):
        """ Constructs the LETF trade.
        """

        letf_stock = random.choice(self._stocks)
        amount = np.random.rand() * 100  # rand() is between 0-1.
        letf_position = {
            'LETF': {
                'trade_id': str(trade_nb),  # str(uuid4()),
                'stock': letf_stock,
                'amount': amount,
                'beta': self._beta,
                'stock_value': 30.,  # TODO: WHAT IS THIS SUPPOSED TO BE???
            }
        }
        return letf_position

    def publish_few_values(self, nb_letfs: int, trade_nb_start: int):
        for letf_idx in range(nb_letfs):
            self.publish_value(
                self._value_1(trade_nb_start + letf_idx * 5)
            )

    def _value_to_publish(self, sleep_between_publish=11.):
        """ Keeps generating new fictitious market for stocks.

        :param sleep_between_publish: amount of time to sleep between
                    publishing.
        """

        trade_nb = self._trade_nb_start
        while True:
            letf_position = self._value_1(trade_nb)
            yield letf_position
            trade_nb += 5
            sleep(sleep_between_publish)
