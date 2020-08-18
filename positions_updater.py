import time

from json    import dumps
from nanomsg import Socket

from rm.socket_msg import NanoSocketMixin, NNGSocketMixin

from kafka import KafkaProducer
from time  import sleep


class PositionUpdaterKafka:

    def __init__(self, server_name : str = 'localhost', port : int = 9092):
        self._producer = KafkaProducer(bootstrap_servers='{0}:{1}'.format(server_name, str(port)))

    def start(self):
        """ Fictional producer

        :return:
        """

        while True:
            self._producer.send('quickstart-events', value=b'TERRIBLE')
            sleep(1)


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
