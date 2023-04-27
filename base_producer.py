""" Base producer functionality.
"""

import logging
# IMPORTANT: This configuration _HAS_ to be here on top.
logging.basicConfig(filename='/tmp/base_producer_service.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import datetime
import random

from typing import Optional, Dict, Tuple, Union, List
from uuid import uuid4, UUID
from threading import Thread
from time import sleep
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord
from json import loads, dumps


class BaseProducer:
    """ Publishes market to a topic.
    """

    def __init__(
            self,
            server_port_topic: Tuple[str, str, str] = ('localhost', 9092, 'letf.mkt', ),
    ):
        server_name, port, mkt_topic = server_port_topic
        bootstrap_servers = f'{server_name}:{port}'

        self._mkt_producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self._mkt_producer_topic = mkt_topic

    def _producer_thread(self, sleep_delay = 1.):
        """ Base producer thread.

        :param sleep_delay: delay before the next produced value.

        """

        for value in self._value_to_publish():
            print("Publishing value.")

            self._mkt_producer.send(
                self._mkt_producer_topic,
                value=bytearray(str(self._value_to_publish()), 'ascii'),
            )

    def _value_to_publish(self):
        """ Generator to generate new values.
        """

        raise NotImplementedError('Need to implement _value_to_publish')


    def run(self) -> Thread:
        """ Runs the thread for market publishing

        2 threads are ran:
           1. _update_new_mkt_events: collects market events and updates the new market.
           2. _operate_markets: holds the current and new market, and switches between them.

        returns: market events thread, switch market thread.
        """

        # market event topic reading thread
        market_events = Thread(target=self._producer_thread)
        market_events.start()

        return market_events
