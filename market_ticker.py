# produces market events

from time  import sleep
from kafka import KafkaProducer
from threading import Thread


class MarketUpdater:

    def __init__( self
                , server_name : str = 'localhost'
                , port        : int = 9092
                , topic       : str = 'quickstarter-events' ):

        self._topic    = topic
        self._producer = KafkaProducer(bootstrap_servers='{0}:{1}'.format(server_name, str(port)))

    def _message(self):
        # return b'MARKET EVENT'
        raise NotImplementedError('Implement _message.')

    def _run_fct(self, sleep_delay : float = 0.1):
        """ Market producer function.

        :returns:
        """

        while True:
            self._producer.send(self._topic, value=self._message())
            sleep(sleep_delay)

    def start(self, idle_delay : float = 0.1) -> Thread:
        """ Run the controller.

        :param idle_delay: delay of the IDLE state of the controller.
        :returns: run the controller.
        """

        run_thread = Thread(target = self._run_fct, kwargs={'idle_delay': idle_delay} )
        run_thread.start()

        return run_thread
