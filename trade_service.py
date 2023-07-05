""" Service for market snapper.
"""

import logging
# IMPORTANT: This configuration _HAS_ to be here on top.
logging.basicConfig(filename='/tmp/market_service.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import datetime
import random
import numpy as np

from typing import Optional, Dict, Tuple, Union, List
from uuid import uuid4
from time import sleep
from json import dumps

from rm.base_producer import BaseProducer


class LETFTradeProducer(BaseProducer):

    def __init__(
            self,
            stocks : List[str],
            server_port_topic: Tuple[str, str, str] = ('localhost', 9092, 'letf.positions'),
            beta = 2.,
            market_producer = None,
    ):
        """ Initializer of the

        """
        super().__init__(server_port_topic)
        self._stocks = stocks  # letf stocks which LETF position can be generated.
        self._beta = beta
        self._market_producer = market_producer

    def _value_to_publish(self, sleep_between_publish=11.):
        """ Keeps generating new fictitious market for stocks.
        """

        while True:

            letf_stock = random.choice(self._stocks)
            amount = np.random.rand() * 100  # rand() is between 0-1.
            letf_position = {
                'LETF': {
                    'trade_id': str(uuid4()),
                    'stock': letf_stock,
                    'amount': amount,
                    'beta': self._beta,
                    'stock_value': None,   #if self._market_producer is None else self._market_producer._curr_stocks.get(letf_stock),
                }
            }

            yield letf_position

            sleep(sleep_between_publish)
