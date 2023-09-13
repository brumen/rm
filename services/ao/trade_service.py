""" Trade producer for the AO positions, simplified.
"""

import sys
import logging
logger = logging.getLogger(__name__)

sys.path.append('/home/brumen/work/')

from rm.trade_service import AOTradeProducer


# start the leveraged etf market producer
ao_producer = AOTradeProducer(flight_ids=['UA150', 'UA155', ])

trade_thread = ao_producer.run(sleep_between_publish=1.)
