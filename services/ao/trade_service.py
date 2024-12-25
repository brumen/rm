""" Trade producer for the AO positions, simplified.
"""

import sys
import logging
import six.moves

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from rm.trade_service import AOTradeProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# start the leveraged etf market producer
ao_producer = AOTradeProducer(
    flight_ids=['UA150', 'UA155', ],
    server_port_topic=(
        '192.168.1.107',
        9092,
        'air_options.ao.flights_live',
    ),
)

trade_thread = ao_producer.run(sleep_between_publish=1.)
