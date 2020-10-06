# Kafka Position updater

import logging

from time      import sleep
from kafka     import KafkaProducer
from threading import Thread

from rm.producer_base import ProducerBase

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class PositionUpdaterJoke(ProducerBase):

    def _message(self):
        return b'POSITION_1'


# pu = PositionUpdaterJoke()
# pu.start(idle_delay=0.1)
