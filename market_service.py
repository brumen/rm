""" Load simulator for market.
"""

import datetime
import sys
import six.moves
import numpy as np

from logging import getLogger
from typing import Optional, Dict, Tuple
from uuid import uuid4, UUID
from threading import Thread
from time import sleep
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.consumer.fetcher import ConsumerRecord


logger = getLogger(__name__)


if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves


class MarketService:
    """ Gathering information about the market. Runs on a ticker.
        For a certain period of time,
        Market data is collected from the topic air_options.ao.flights_live.
        It is then published on the topic mkt_events in an encoded fashion.
        Market is shown on .new_market property.
    """

    def __init__(
            self,
            flights: Optional = None,
            time_interval: int = 5,
            server_port_topic: Tuple[str, str, str] = (
                'localhost',
                9092,
                'air_options.ao.flights_live',
            ),
            mkt_events_topic: str = 'air_options.ao.mkt_events',
    ):
        """

        :param flights: flights to be in the market. If None, all
            flights are scheduled.
        :param time_interval: time interval in seconds between two consecutive
            markets.
        :param server_port_topic: tuple of (name of the kafka server, port nb,
            topic to read from) where the
            market related information is read from.
        :param mkt_events_topic: topic where the UUID of the market is
            published.
        """

        self.flights = flights
        self.time_interval = time_interval

        server_name, port, mkt_topic = server_port_topic
        server_port = f'{server_name}:{port}'
        while True:  # we need mkt listener
            try:
                self.__mkt_listener = KafkaConsumer(
                    bootstrap_servers=server_port
                )
                break
            except NoBrokersAvailable as e:
                logger.warn(
                    f'No kafka broker on {server_port_topic}. Attempting in 5 secs: {e}'
                )
                sleep(5)

        self.__mkt_listener.assign(
            [TopicPartition(topic=mkt_topic, partition=0)]
        )
        self.__mkt_listener.seek_to_beginning()

        # producer of market events
        while True:
            try:
                self.__mkt_producer = KafkaProducer(
                    bootstrap_servers=server_port
                )
                break
            except NoBrokersAvailable as e:
                logger.warn(
                    f'No kafka broker on {server_port_topic}. Attempting in 5 secs: {e}'
                )
                sleep(5)

        self.__mkt_producer_topic = mkt_events_topic

        self.__prev_market = {}  # both markets are empty
        self.__new_market_updates = {}
        self.__prev_market_id = uuid4()  # ID of the previous market
        self.__new_market_id = uuid4()  # ID of the new market
        self.__new_market_snap_time = datetime.datetime.now()

    def _update_new_mkt_events(self):
        """ Worker function processing new events, which eventually
            comprise new market snap.

        Function doesnt return anything, just updates self.__new_market_updates
        """

        for msg in self.__mkt_listener:
            logger.info('New market event.')
            self.__new_market_updates.update(self._process_mkt_msg(msg))

    def _process_mkt_msg(self, msg: ConsumerRecord) -> Dict:
        """ Processes the market message.

        :returns: processed market message.
        """

        raise NotImplementedError('Implement the _process_mkt_msg')

    @property
    def latest_market(self) -> Tuple[UUID, Dict]:
        """ Gets the latest market id and market.

        :returns: latest market id and latest market.
        """

        return self.__prev_market_id, self.__prev_market

    def encode_mkt(
            self,
            mkt_to_encode: Dict[Tuple[str, datetime.date], float]
    ) -> str:
        """ Encodes the latest market to be sent over json
            encoding is in the form ('UA79', datetime.date(2022, 1, 2)) -> 'UA79|20220101'
            using %Y%m%d encoding for date.

        :returns: encoded market in the format above.
        """

        raise NotImplementedError('Need to implement the encode_mkt method.')

    def _shock_latest_market(self, shock_factor: 1.) -> Dict[Tuple[str, datetime.date], float]:
        _, latest_mkt = self.latest_market

        return {
            flight_info: price * shock_factor
            for flight_info, price in latest_mkt.items()
        }

    def _operate_markets(
            self,
            sleep_delay=0.2,
            testing_shift=(1., 1., )
    ) -> None:
        """ Switch markets every time_interval seconds.

        :param sleep_delay: only issue a new market every sleep_delay seconds.
        :param testing_shift: alternate between multiplying the market by
            first or second element.
            IMPORTANT: this latter just for testing purposes.
        """

        while True:
            elapsed_time = (
                datetime.datetime.now() -
                self.__new_market_snap_time
            ).seconds

            if elapsed_time >= self.time_interval:
                # switch: curr_market <- new_market
                logger.debug(f'Elapsed time: {elapsed_time}')
                logger.info(
                    f'Switching market from {self.__prev_market_id} to '
                    f'{self.__new_market_id}'
                )
                # updated market
                self.__prev_market |= self.__new_market_updates

                self.__new_market_updates = {}  # reset new market updates.

                self.__prev_market_id = self.__new_market_id
                self.__new_market_id = uuid4()
                self.__new_market_snap_time = datetime.datetime.now()

                _, latest_mkt = self.latest_market
                current_shock = np.random.random()
                shocked_mkt = self._shock_latest_market(
                    shock_factor=current_shock,
                )
                mkt_sent = self.encode_mkt(shocked_mkt)
                logger.debug(f"_operate_markets: Market sent: {mkt_sent}")

                # send the updated market to the mkt_events topic.
                logger.info("Sending new market to mkt_events topic.")
                self.__mkt_producer.send(
                    self.__mkt_producer_topic,
                    value=bytearray(str(mkt_sent), 'ascii'),
                )

            else:
                sleep(sleep_delay)

    def run(
            self,
            sleep_delay=5,
            testing_shift=(1., 1.)
    ) -> Tuple[Thread, Thread]:
        """ Runs the threads for market operation.

        2 threads are ran:
           1. _update_new_mkt_events: collects market events and
                  updates the new market.
           2. _operate_markets: holds the current and new market, and
                   switches between them.

        :params sleep_delay: sleep delay
        :params testing_shift: shift to multiply the markets with.
        returns: market events thread, switch market thread.
        """

        # this thread collects updates to the market.
        market_events = Thread(target=self._update_new_mkt_events)
        market_events.start()

        # this thread emits the new market every 5 seconds.
        switch_markets = Thread(
            target=self._operate_markets,
            kwargs={
                'sleep_delay': sleep_delay,
                'testing_shift': testing_shift,
            }
        )
        switch_markets.start()

        return market_events, switch_markets
