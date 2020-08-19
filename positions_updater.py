import time

from json    import dumps
from nanomsg import Socket
from threading import Thread

from rm.socket_msg import NanoSocketMixin, NNGSocketMixin

from kafka import KafkaProducer
from time  import sleep


class PositionUpdaterKafka:

    def __init__( self
                , server_name : str = 'localhost'
                , port : int        = 9092
                , topic : str       = 'quickstart-events' ):

        self._producer = KafkaProducer(bootstrap_servers='{0}:{1}'.format(server_name, str(port)))
        self._topic    = topic

    def _message(self):
        # return b'TERRIBLE'
        raise NotImplementedError('Implement the _message method.')

    def _run_fct(self, sleep_delay : float = 0.1):
        """ Producer

        :param sleep_delay: sleep delay for the producer
        :return:
        """

        while True:
            self._producer.send(self._topic, value=self._message)
            sleep(sleep_delay)

    def start(self, idle_delay : float = 0.1) -> Thread:
        """ Run the controller.

        :param idle_delay: delay of the IDLE state of the controller.
        :returns: run the controller.
        """

        run_thread = Thread(target = self._run_fct, kwargs={'idle_delay': idle_delay} )
        run_thread.start()

        return run_thread


class PositionUpdater:
    """ Handles positions updating - publishes on position_socket.
    """

    def __init__( self, pub_socket : Socket ):
        """ Position updater is a publisher of new/deleted/changed positions from the database.

        :param pub_socket: position_socket to publish the positions.
        """

        self._pub_socket  = pub_socket

    @classmethod
    def from_host(cls, db_host = '127.0.0.1', pub_port = 5556):
        """ Constructs the class from host & port where to update positions.
        """

        return cls( NNGSocketMixin.create_socket(pub_port, pub_sub='pub', host=db_host) )

    def start(self, sleep_time = .3) -> None:
        """ Sends the position to the controller, acts as a publisher.
        """

        raise NotImplementedError('Position updater class should overwrite the start method.')


class PositionUpdaterAO(PositionUpdater):
    """ Working class of the position updater of Air options.
    """

    def start(self, sleep_time = .3) -> None:

        while True:
            self._pub_socket.send(dumps({'event_type': 'delete_trade', 'trade_nb': 2}))
            time.sleep(sleep_time)
            self._pub_socket.send(dumps({'event_type': 'new_trade', 'trade_nb': 1}))


if __name__ == '__main__':
    pu = PositionUpdater.from_host()
    pu.start(.02)
