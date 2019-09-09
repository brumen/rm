# listens to the queries from the controller.

import logging
from threading import Thread
from nanomsg import Socket

from socket_msg import NanoSocketMixin
from encode_decode import EncodeDecodeMixin

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class DeltaListener(EncodeDecodeMixin):
    """ Subscribes to a port & reports the results.
    """

    def __init__( self
                , sub_socket    : Socket
                , ):
        """ Position updater is a publisher of new/deleted/changed positions from the database.

        :param sub_socket: subscribe socket to listen to delta.
        """

        self._sub_socket  = sub_socket
        self.__current_delta = None

    @classmethod
    def from_host(cls, db_host = '127.0.0.1', sub_port = 5720):
        """ Constructs the class from host & port where to update positions.
        """

        return cls( NanoSocketMixin._create_socket(port=sub_port, pub_sub='sub', host=db_host) )

    def _report_delta(self):
        """ Reports the delta received.
        """

        while True:
            delta = self._decode_message(self._sub_socket.recv())
            if delta != self.__current_delta:
                logger.info('Delta received = {0}'.format(delta))
                self.__current_delta = delta

    def start( self
             , sleep_time = .3  ) -> None:
        """ listens to the controller's results.
        """
        Thread(target=self._report_delta).start()


if __name__ == '__main__':
    dl = DeltaListener.from_host()
    dl.start(.0002)
