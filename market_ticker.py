# produces market events

import logging

from rm.producer_base import ProducerBase


logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class MarketUpdaterJoke(ProducerBase):

    def _message(self):
        return b'MARKET_EVENT_1'


# mu = MarketUpdaterJoke()
# mu.start(idle_delay=5)
