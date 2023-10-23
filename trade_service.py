""" Service for market snapper.
"""

import datetime
import random
import numpy as np
from logging import getLogger
from time import sleep

from typing import Tuple, List
from uuid import uuid4
from time import sleep
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

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
            mkt_producer=None,
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


class AOTradeProducer(BaseProducer):
    """

    """
    def __init__(
            self,
            flight_ids: List[str] = [1, 2, 3],
            server_port_topic: Tuple[str, str, str] = (
                'localhost',
                9092,
                'air_options.ao.flights_live',
            ),
    ):
        self.flight_ids = flight_ids

        server_name, port, value_topic = server_port_topic
        bootstrap_servers = f'{server_name}:{port}'

        self._value_producer_topic = value_topic

        while True:
            try:
                self._value_producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=self._serialize_msg,
                )
                break
            except NoBrokersAvailable as e:
                logger.warn(
                    f'No Kafka broker on {bootstrap_servers}. Attempting in 5 secs: {e}'
                )
                sleep(5)

    def _value_to_publish(self, sleep_between_publish=11.):

        while True:

            for flight in self.flight_ids:
                letf_position = {
                    'carrier_nb': flight,
                    'dep_date': datetime.date(2017, 5, 1).strftime('%Y%m%d'),
                    'price': 150.,
                }

                yield letf_position

            sleep(sleep_between_publish)
