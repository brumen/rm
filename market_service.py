""" Service for market snapper.
"""

import datetime
import logging

from typing    import Optional, Dict, Tuple, Union
from uuid      import uuid4, UUID
from threading import Thread
from time      import sleep
from kafka     import KafkaConsumer, TopicPartition, KafkaProducer
from kafka.consumer.fetcher import ConsumerRecord
from json      import loads

logging.basicConfig(filename='/tmp/market_service.log')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class MarketEncodeDecodeMixin:
    """ Encodes and decodes the market.
    """

    @staticmethod
    def encode_from_tuple(encode_d : Dict[Tuple[str, datetime.date], float]) -> Dict[str, float]:
        """ Encodes the dictionary of the form (str, datetime.date): float into a dictionary
            of Dict[str, float], by combining the str and datetime into a string.

        :param encode_d: dictionary to encode w/ | for the tuple.
        :returns: resulting encoded dictionary.
        """

        return { f"{flight_id}|{flight_date.strftime('%Y%m%d')}": flight_price
                 for (flight_id, flight_date), flight_price in encode_d.items() }

    @staticmethod
    def decode_to_tuple(enc_str_date : str) -> Union[None, Tuple[str, datetime.date]]:
        """ Decodes the encoded (flight_id, flight_date) to this state.

        If the conversion fails, None is returned.

        :param enc_str_date: encoded (flight_id, flight_date) in the format described in _encode_mkt
        :returns: decoded flight_id, flight_date, or None if there is an error in
        """

        try:
            flight_id, flight_date_enc = enc_str_date.split('|')
        except Exception as e:
            logger.warning(f'Could not convert {enc_str_date}, continuing and ignoring the element: {e}')
            return None

        try:
            return flight_id, datetime.datetime.strptime(flight_date_enc, '%Y%m%d').date()
        except Exception as e:
            logger.warning(f'Could not convert the date to the datetime.date structure: {e}')
            return None

    @classmethod
    def decode_mkt(cls, encoded_id_mkt : Tuple[UUID, Dict[str, float]]) -> Dict[Tuple[str, datetime.date], float]:
        """ Decodes the encoded market w/ the encode_mkt function above.

        :param encoded_mkt: encoded market to be decoded.
        :return: decoded market in a more reasonable form.
        """

        _, encoded_mkt = encoded_id_mkt

        return {cls.decode_to_tuple(encoded_nb_date) : flight_price
                for encoded_nb_date, flight_price in encoded_mkt.items() }

    # TODO: DO THESE METHODS BELONG HERE???
    @classmethod
    def encode_mkt(cls, latest_id_market : Tuple[UUID, Dict[Tuple[str, datetime.date], float]]) -> Tuple[UUID, Dict[str, float]]:
        """ Encodes the latest market to be sent over json
            encoding is in the form ('UA79', datetime.date(2022, 1, 2)) -> 'UA79|20220101'
            using %Y%m%d encoding for date.

        :returns: encoded market in the format above.
        """

        if latest_id_market is None:
            logger.warning('Market not yet computed. Wait a bit')
            return 'noname_id', {}

        # market is computed, decipher it.
        latest_market_id, latest_market = latest_id_market  # TODO: FIX THE NAMING CONVENTION

        return latest_market_id, cls.encode_from_tuple(latest_market)



# TODO: USE ProducerBase class here
class MarketService:
    """ Gathering information about the market. Runs on a ticker. For a certain period of time,
        Market data is collected from the topic air_options.ao.flights_live.
        It is then published on the topic mkt_events in a encoded fashion.
        Market is shown on .new_market property.
    """

    def __init__( self
                , flights           : Optional = None
                , time_interval     : int      = 5
                , server_port_topic : Tuple[str, str, str] = ('localhost', 9092, 'air_options.ao.flights_live', )
                , mkt_events_topic  : str = 'mkt_events'
                , ):
        """

        :param flights: flights to be in the market. If None, all flights are scheduled.
        :param time_interval: time interval in seconds between two consequitive markets.
        :param server_port_topic: tuple of (name of the kafka server, port nb, topic to read from) where the market related
                     information is read from.
        :param mkt_events_topic: topic where the UUID of the market is published.
        """

        self.flights       = flights
        self.time_interval = time_interval

        server_name, port, mkt_topic = server_port_topic
        server_port = f'{server_name}:{port}'
        self.__mkt_listener = KafkaConsumer(bootstrap_servers=server_port)
        self.__mkt_listener.assign([TopicPartition(topic=mkt_topic, partition=0)])
        self.__mkt_listener.seek_to_beginning()

        # producer of market events
        self.__mkt_producer = KafkaProducer(bootstrap_servers=server_port)
        self.__mkt_producer_topic = mkt_events_topic

        self.__prev_market          = {}  # both markets are empty
        self.__new_market_updates   = {}
        self.__prev_market_id       = uuid4()  # ID of the previous market
        self.__new_market_id        = uuid4()  # ID of the new market
        self.__new_market_snap_time = datetime.datetime.now()

    def _update_new_mkt_events(self):
        """ Worker function processing new events, which eventually comprise new market snap.

        Function doesnt return anything, just updates self.__new_market_updates
        """

        for msg in self.__mkt_listener:
            self.__new_market_updates.update(self._process_mkt_msg(msg))

    def _process_mkt_msg(self, msg : ConsumerRecord) -> Dict:
        """ Processes the market message.

        :returns: processed market message.
        """

        raise NotImplementedError('Implement the _process_mkt_msg')

    def _elapsed_time(self) -> int :
        """ The amount of time elapsed since the new market has started filling

        :return:
        """

        return (datetime.datetime.now() - self.__new_market_snap_time).seconds

    @property
    def latest_market(self) -> Tuple[UUID, Dict]:
        """ Gets the latest market id and market.

        :returns: latest market id and latest market.
        """

        return self.__prev_market_id, self.__prev_market

    def _operate_markets(self):
        """ Switch markets every time_interval seconds.

        :return:
        """

        while True:
            if self._elapsed_time() > self.time_interval:  # switch: curr_market <- new_market
                logger.info(f'Switching market from {self.__prev_market_id} to {self.__new_market_id}')
                self.__prev_market |= self.__new_market_updates  # updated market
                self.__new_market_updates = {}  # reset new market updates.

                self.__prev_market_id = self.__new_market_id
                self.__new_market_id = uuid4()
                self.__new_market_snap_time = datetime.datetime.now()
                # TODO: THIS BELOW IS BAD
                self.__mkt_producer.send(self.__mkt_producer_topic, value=bytearray(str(self.__prev_market_id),
                                                                                    'ascii'))  # send an update to the market topic
            else:
                sleep(1)  # sleep for a second.

    def run(self) -> Tuple[Thread, Thread]:
        """ Runs the threads for market operation.

        2 threads are ran:
           1. _update_new_mkt_events: collects market events and updates the new market.
           2. _operate_markets: holds the current and new market, and switches between them.

        returns: market events thread, switch market thread.
        """

        market_events = Thread(target=self._update_new_mkt_events)  # market event topic reading thread
        market_events.start()

        switch_markets = Thread(target=self._operate_markets)  # market event topic reading thread
        switch_markets.start()

        return market_events, switch_markets


class AOMarketService(MarketService, MarketEncodeDecodeMixin):
    """ Market service with decode/encode features.
    """

    def _process_mkt_msg(self, msg : ConsumerRecord) -> Dict[Tuple[str, datetime.date], float]:
        """ Snaps the market at a particular time.

        :param msg: message from kafka connect, in the form:
        {'before': None,
         'after': {'as_of': 1511085283000,
          'orig': 'EWR',
          'dest': 'SFO',
          'price': 547.8,
          'flight_id': '11442-1712122029--31722-0-16216-1712130001',
          'dep_date': 17512,
          'dep_time': 73740000000,
          'arr_date': 1513123260000,
          'carrier': 'UA',
          'flight_nb': '06',
          'cabin_class': 'economy',
          'flights_primary_id': 694},
         'source': {'version': '1.3.1.Final',
          'connector': 'mysql',
          'name': 'air_options',
          'ts_ms': 0,
          'snapshot': 'last',
          'db': 'ao',
          'table': 'flights_live',
          'server_id': 0,
          'gtid': None,
          'file': 'mysql-bin.000682',
          'pos': 156,
          'row': 0,
          'thread': None,
          'query': None},
         'op': 'c',
         'ts_ms': 1641993317788,
         'transaction': None}

        :returns: a tuple of flight_id, flight expiry, flight price
        """

        msg_d = loads(msg.value)

        msg_payload = msg_d.get('payload')
        if msg_payload.get('op') == 'c':  # create, only look at 'after'
            flight_info = msg_payload['after']
            flight_carrier = flight_info.get('carrier')
            flight_nb      = flight_info.get('flight_nb')
            flight_date    = datetime.date(1970, 1, 1) + datetime.timedelta(days=flight_info.get('dep_date'))  # TODO: DAYS after 1970/1/1

            return {(f'{flight_carrier}{flight_nb}', flight_date): flight_info.get('price')}

    def encode_mkt(self) -> Tuple[UUID, Dict[str, float]]:
        """ Encodes the latest market to be sent over json
            encoding is in the form ('UA79', datetime.date(2022, 1, 2)) -> 'UA79|20220101'
            using %Y%m%d encoding for date.

        :returns: encoded market in the format above.
        """

        latest_market_id, latest_market = self.latest_market

        return latest_market_id, self.encode_from_tuple(latest_market)

    def decode_mkt(self, encoded_mkt : Tuple[UUID, Dict[str, float]]) -> Tuple[UUID, Dict[Tuple[str, datetime.date], float]]:
        """ Decodes the previously encoded market.

        :param encoded_mkt: market encoded with the encode_mkt function.
        :return: market in a more reasonable form.
        """

        mkt_id, encoded_market = encoded_mkt

        return mkt_id, {self.decode_to_tuple(encoded_nb_date) : flight_price
                        for encoded_nb_date, flight_price in encoded_market.items() }
