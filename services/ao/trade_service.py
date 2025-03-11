""" Load simulator for AO trades.
"""

import sys
import logging
import six.moves
import datetime
import numpy as np
import os

from dotenv import load_dotenv
from typing import Tuple, List
from time import sleep

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from rm.base_producer import BaseProducer


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        """ Updating AO flights with some values. Just for testing purposes
            for now.
        """

        while True:

            for flight in self.flight_ids:
                ao_position = {
                    'carrier_nb': flight,
                    'dep_date': datetime.date(2017, 5, 1).strftime('%Y%m%d'),
                    'price': 150. + np.random.uniform(high=50.),
                }

                logger.info(
                    f'Publishing AO flight position: {ao_position}'
                )

                yield ao_position

            sleep(sleep_between_publish)


def _main():
    # start the leveraged etf market producer
    load_dotenv()
    host = os.getenv('HOST')
    kafka_port = os.getenv('KAFKA_PORT')

    ao_producer = AOTradeProducer(
        flight_ids=['UA150', 'UA155', ],
        server_port_topic=(
            host,
            int(kafka_port),
            'air_options.ao.flights_live',
        ),
    )

    ao_producer.run(sleep_between_publish=1.)


# whether to run or not.
_main()
