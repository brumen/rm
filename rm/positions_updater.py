import time

from socket_msg import NanoSocketMixin


class PositionUpdater:
    """ Handles positions updating.

    """

    def __init__( self
                , socket ):
        """ Init position updater.

        :param db_name: database name, e.g. 'ao'
        :param db_host: database host, e.g. 'localhost'
        :param port: port for reporting updates.
        """

        self.__socket  = socket

    @classmethod
    def from_host(cls, db_host = '127.0.0.1', port = 5556):
        """ Constructs the class from host & port where to update positions.
        """

        return cls(NanoSocketMixin._create_socket(port, pub_sub='pub', host=db_host))

    def start( self
             , sleep_time = .3  ) -> None:
        """ Sends the position to the controller,
            acts as a publisher.

        # TODO: FINISH THIS, NOW IT'S JUST FAKE.
        """

        while True:
            self.__socket.send_json({'event_type': 'delete_trade', 'trade_nb': 2})
            time.sleep(sleep_time)
            self.__socket.send_json({'event_type': 'new_trade', 'trade_nb': 1})


if __name__ == '__main__':
    pu = PositionUpdater()
    pu.start()
