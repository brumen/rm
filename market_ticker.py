# produces market events

from rm.in_out_updater import InOutUpdater
from rm.input_classes  import SocketInputSource

from time  import sleep
from kafka import KafkaProducer


class MarketUpdater:

    def __init__(self, server_name : str = 'localhost', port : int = 9092):
        self._producer = KafkaProducer(bootstrap_servers='{0}:{1}'.format(server_name, str(port)))

    def start(self):
        """ Fictional producer

        :return:
        """

        while True:
            self._producer.send('quickstart-events', value=b'MARKET_EVENT')
            sleep(1)
