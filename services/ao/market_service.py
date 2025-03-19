""" The actual rester for the market service.
    The rester service is on: localhost:5000/mkt/get_market
    The market reported is a tuple, with two elements:
       1.st: uuid4 of the current market ("5342234234kasdasda-asd-asdasd-")
       2nd: dictionary where keys are flight_nb|departure_date, values are prices
            key = "UA06|20170608"; value=303
    Market service publishes on mkt_events topic, mkt event is the uuid4 described above.
"""

import logging
import sys
import six.moves
import datetime

from uuid import UUID
from typing import Dict, Tuple, Union

# from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from json import loads, dumps


if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

# these two things have to be below sys.modules setup
from kafka.consumer.fetcher import ConsumerRecord
from rm.market_service import MarketService


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AOMarketService(MarketService):
    """ Market service with decode/encode features.
    """

    def _process_mkt_msg(
            self,
            msg: ConsumerRecord
    ) -> Dict[Tuple[str, datetime.date], float]:
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

        msg_payload = msg_d.get('payload')  # TODO: THIS CAN BE None

        if msg_payload is None:
            logger.warn('Could not get market payload. Ignoring message')
            return {}

        if msg_payload.get('op') == 'c':  # create, only look at 'after'
            flight_info = msg_payload['after']
            flight_carrier = flight_info.get('carrier')
            flight_nb = flight_info.get('flight_nb')
            # TODO: below DAYS after 1970/1/1
            flight_date = datetime.date(1970, 1, 1) + \
                datetime.timedelta(days=flight_info.get('dep_date'))

            return {
                (f'{flight_carrier}{flight_nb}', flight_date):
                flight_info.get('price')
            }

    @staticmethod
    def encode_from_tuple(
            encode_d: Dict[Tuple[str, datetime.date], float]
    ) -> Dict[str, float]:
        """ Encodes the dictionary of the form (str, datetime.date):
            float into a dictionary
            of Dict[str, float], by combining the str and datetime
            into a string.

        :param encode_d: dictionary to encode w/ | for the tuple.
        :returns: resulting encoded dictionary.
        """

        return {f"{flight_id}|{flight_date.strftime('%Y%m%d')}": flight_price
                for (flight_id, flight_date), flight_price in encode_d.items()}

    @staticmethod
    def decode_to_tuple(
            enc_str_date: str
    ) -> Union[None, Tuple[str, datetime.date]]:
        """ Decodes the encoded (flight_id, flight_date) to this state.

        If the conversion fails, None is returned.

        :param enc_str_date: encoded (flight_id, flight_date) in the
            format described in _encode_mkt
        :returns: decoded flight_id, flight_date, or None if there
            is an error in
        """

        try:
            flight_id, flight_date_enc = enc_str_date.split('|')

        except Exception as e:
            logger.warning(
                f'Could not convert {enc_str_date}, continuing '
                f'and ignoring the element: {e}')
            return None

        try:
            flight_date = datetime.datetime.strptime(
                flight_date_enc, '%Y%m%d'
            ).date()
            return flight_id, flight_date

        except Exception as e:
            logger.warning(
                f'Could not convert the date to the datetime.date '
                f'structure: {e}'
            )
            return None

    def encode_mkt(
            self,
            mkt_to_encode: Dict[Tuple[str, datetime.date], float],
    ) -> str:
        """ Encodes the latest market and market id in json format, to
                be sent to kafka
                encoding is in the form
                ('UA79', datetime.date(2022, 1, 2)) -> 'UA79|20220101'
            using %Y%m%d encoding for date.

        :returns: encoded market in the format above.
        """

        return dumps(self.encode_from_tuple(mkt_to_encode))

    @classmethod
    def decode_mkt_data(
            cls,
            encoded_mkt: Dict[str, float],
    ) -> Dict[Tuple[str, datetime.date], float]:

        return {
            cls.decode_to_tuple(encoded_nb_date): flight_price
            for encoded_nb_date, flight_price in encoded_mkt.items()
        }

    @classmethod
    def decode_mkt(
            cls,
            encoded_id_mkt: Tuple[UUID, Dict[str, float]],
    ) -> Dict[Tuple[str, datetime.date], float]:
        """ Decodes the encoded market w/ the encode_mkt function above.

        :param encoded_id_mkt: market_id, and encoded market as a tuple.
        :return: decoded market in a more reasonable form.
        """

        _, encoded_mkt = encoded_id_mkt

        return cls.decode_mkt_data(encoded_mkt)


class AOMarketServiceLocal(AOMarketService):
    """ Market service with decode/encode features.
    """

    def _process_mkt_msg(
            self,
            msg: ConsumerRecord
    ) -> Dict[Tuple[str, datetime.date], float]:
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

        flight_info = loads(msg.value)
        flight_carrier_nb = flight_info.get('carrier_nb')

        if flight_carrier_nb is None:
            flight_carrier_nb = 'UA160'

        flight_date: datetime.date = datetime.date(2017, 5, 1)  # flight_info.get('dep_date').strptime("%Y%m%d")
        flight_price = flight_info.get('price')
        if flight_price is None:
            flight_price = 200.  # + np.random.

        return {(flight_carrier_nb, flight_date): flight_price}


def _main():
    # starting the service
    aom = AOMarketServiceLocal(
        server_port_topic=(
            '192.168.1.107',
            9092,
            'air_options.ao.flights_live',
        ),
        time_interval=1,
    )
    aom.run(sleep_delay=0.2, testing_shift=(1., 5.))


# starting the market load simulator.
# _main()
