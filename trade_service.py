""" Service for market snapper.
"""

import random
import numpy as np
import redis
from logging import getLogger

from typing import Tuple, List
from uuid import uuid4
from time import sleep

from rm.base_producer import BaseProducer


logger = getLogger(__name__)


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

        # also initialize the redis connector.
        #self._redis_cli = redis.Redis(
        #    host='localhost',
        #    port=6379,
        #    decode_responses=True,
        #)

    # def _producer_thread(self, sleep_delay=11.):
    #     """ Base producer thread.

    #     :param sleep_delay: delay before the next produced value.
    #     """

    #     for letf_trade in self._value_to_publish(
    #             sleep_between_publish=sleep_delay
    #     ):
    #         # value is of the dict form 'LETF': { ... params }
    #         logger.debug(f'_producer_thread: Publishing letf_trade {letf_trade}.')

    #         letf_inner = letf_trade['LETF']
    #         # first publish this to 
    #         self._redis_cli.set(letf_inner.id).value(letf_trade)

    #         self._mkt_producer.send(
    #             self._mkt_producer_topic,
    #             value=letf_trade
    #         )

    def _value_to_publish(self, sleep_between_publish=11.):
        """ Keeps generating new fictitious market for stocks.

        :param sleep_between_publish: amount of time to sleep between
                    publishing.
        """

        trade_nb = 0

        while True:

            letf_stock = random.choice(self._stocks)
            amount = np.random.rand() * 100  # rand() is between 0-1.
            letf_position = {
                'LETF': {
                    'trade_id': str(trade_nb),  # str(uuid4()),
                    'stock': letf_stock,
                    'amount': amount,
                    'beta': self._beta,
                    'stock_value': None,
                }
            }

            yield letf_position

            trade_nb += 5
            sleep(sleep_between_publish)
