""" Base producer functionality.
"""

import logging
import json

from typing import Tuple
from threading import Thread
from kafka import KafkaProducer


logger = logging.getLogger(__name__)


class BaseProducer:
    """ Publishes market to a topic.
    """

    def __init__(
            self,
            server_port_topic: Tuple[str, str, str] = (
                'localhost',
                9092,
                'letf.mkt',
            ),
    ):
        server_name, port, value_topic = server_port_topic
        bootstrap_servers = f'{server_name}:{port}'

        self._value_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=self._serialize_msg,
        )

        self._value_producer_topic = value_topic

    @staticmethod
    def _serialize_msg(m):
        return json.dumps(m).encode('utf-8')

    def _producer_thread(self, sleep_delay=11.):
        """ Base producer thread.

        :param sleep_delay: delay before the next produced value.
        """

        for value in self._value_to_publish(sleep_between_publish=sleep_delay):
            print(f'Publishing to {self._value_producer_topic}: {value}.')

            self._value_producer.send(
                self._value_producer_topic,
                value=value
            )

    def _value_to_publish(self, sleep_between_publish=None):
        """ Generator to generate new values.

        :param sleep_between_publish: sleep between individual publishing.
        """

        raise NotImplementedError('Need to implement _value_to_publish')

    def create_thread(self, sleep_between_publish=11.) -> Thread:
        return Thread(
            target=self._producer_thread,
            kwargs={'sleep_delay': sleep_between_publish}
        )

    def run(self, sleep_between_publish=11.):
        """ Runs the thread for market publishing

        2 threads are ran:
           1. _update_new_mkt_events: collects market events and updates
                  the new market.
           2. _operate_markets: holds the current and new market, and
                  switches between them.

        :param sleep_between_publish: sleep between individual
           publishing events.
        returns: market events thread, switch market thread.
        """

        # market event topic reading thread
        market_events = self.create_thread(
            sleep_between_publish=sleep_between_publish
        )
        market_events.start()
