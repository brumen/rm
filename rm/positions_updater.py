import time

from socket import SocketMixin


class PositionUpdater(SocketMixin):
    """ Handles positions updating.

    """

    def __init__( self
                , db_name = 'ao'
                , db_host ='localhost'
                , port    = 5556 ):
        """ Init position updater.

        :param db_name: database name, e.g. 'ao'
        :param db_host: database host, e.g. 'localhost'
        :param port: port for reporting updates.
        """

        self.__db_host = db_host
        self.__db_name = db_name
        self.__port    = port
        self.__context, self.__socket = self._create_socket(port, pub_sub='pub')

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
