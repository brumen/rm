# Kafka Position updater

import logging

from rm.producer_base import ProducerBase

logging.basicConfig(filename='/tmp/position_updater.log')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class PositionUpdaterJoke(ProducerBase):

    def _message(self):
        return b'POSITION_1'


# pu = PositionUpdaterJoke()
# pu.start(idle_delay=0.1)
