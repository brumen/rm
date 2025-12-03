""" Setup producer. Sends the messages to the SETUP_TOPIC.
"""

import os
import logging
import json
from dotenv import load_dotenv
from logging import getLogger
from typing import List

logging.basicConfig(level=logging.INFO)

from rm.base_producer import BaseProducer

_logger = getLogger(__name__)

# start the leveraged etf market producer
load_dotenv()
KAFKA_HOST = os.getenv('HOST')  # '192.168.1.107'
KAFKA_PORT = os.getenv('KAFKA_PORT')  # 9092
SETUP_TOPIC = os.getenv('SETUP_TOPIC')

_logger.info(
    f'Starting setup service on {KAFKA_HOST}:{KAFKA_PORT}, '
    f'position_topic: {SETUP_TOPIC}'
)


class SetupProducer(BaseProducer):

    def send_metrics(self, metrics_l: List[str] = ['PV']):
        setup_value = {
            'metrics': metrics_l,
        }

        msg_value = json.dumps(setup_value)

        self._value_producer.send(
            SETUP_TOPIC,
            value=msg_value,
        )


setup_producer = SetupProducer(server_port_topic=('192.168.1.107', 9092, 'letf.setup'))
