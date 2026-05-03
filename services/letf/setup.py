""" Setup producer. Sends the messages to the SETUP_TOPIC.
"""

import os
import logging
from dotenv import load_dotenv
from logging import getLogger
from typing import List, Tuple

logging.basicConfig(level=logging.INFO)

from rm.services.base_producer import BaseProducer

_logger = getLogger(__name__)

# start the leveraged etf market producer
load_dotenv()
# KAFKA_HOST = os.getenv("HOST")  # '192.168.1.107'
# KAFKA_PORT = os.getenv("KAFKA_PORT")  # 9092
# SETUP_TOPIC = os.getenv("SETUP_TOPIC")


class SetupProducer(BaseProducer):

    def __init__(
        self,
        server_port_topic: Tuple[str, str, str] = (
            "192.168.1.50",
            9092,
            "letf.setup",
        ),
    ):
        _, _, self.setup_topic = server_port_topic
        super().__init__(server_port_topic=server_port_topic)

    @classmethod
    def from_env(cls):
        kafka_host = os.getenv("HOST")  # '192.168.1.107'
        kafka_port = os.getenv("KAFKA_PORT")  # 9092
        kafka_setup_topic = os.getenv("SETUP_TOPIC")

        _logger.info(
            f"Starting setup service on {kafka_host}:{kafka_port}, "
            f"position_topic: {kafka_setup_topic}"
        )

        return cls(server_port_topic=(kafka_host, kafka_port, kafka_setup_topic))

    def send_metrics(self, metrics_l: List[str] = ["PV"]):
        setup_value = {
            "Metrics": metrics_l,
        }

        self._value_producer.send(
            self.setup_topic,
            value=setup_value,
        )


if __name__ == "__main__":
    setup_producer = SetupProducer.from_env()
    setup_producer.send_metrics(["PV"])
