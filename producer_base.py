import logging

from time  import sleep
from kafka import KafkaProducer
from threading import Thread
from base64 import encode

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class ProducerBase:

    def __init__( self
                , server_name : str = 'localhost'
                , port        : int = 9092
                , topic       : str = 'market.events' ):

        self._topic    = topic
        self._producer = KafkaProducer(bootstrap_servers='{0}:{1}'.format(server_name, str(port)))

    def _message(self):
        raise NotImplementedError('Implement _message method.')

    @staticmethod
    def _abbreviate_msg(msg) -> str:
        """ Abbreviates the message for reasonable logging.
        """

        raise NotImplementedError('_abbreviate_msg not implemented.')

    def _run_fct(self, sleep_delay : float = 0.1):
        """ Market producer function that is ran as a thread.

        :returns:
        """

        while True:
            message = self._message()
            logger.info(f'Sending new market: {self._abbreviate_msg(message)}')
            self._producer.send(self._topic, value=encode(message))

            sleep(sleep_delay)

    def start(self, idle_delay : float = 0.1) -> Thread:
        """ Run the controller.

        :param idle_delay: delay of the IDLE state of the controller.
        :returns: run the controller.
        """

        run_thread = Thread(target = self._run_fct, kwargs={'sleep_delay': idle_delay} )
        run_thread.start()

        return run_thread
