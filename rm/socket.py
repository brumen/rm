# main controlling logic for the risk management

import zmq
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class SocketMixin:
    """ Socket Mixin.
    """

    def _create_socket(self, port, pub_sub='sub'):
        """ Create socket part.

        """

        context = zmq.Context()
        if pub_sub == 'sub':
            socket  = context.socket(zmq.SUB)
            socket.setsockopt_string(zmq.SUBSCRIBE, '')
            socket.bind("tcp://*:{0}".format(port))  # server ip
        else:
            socket = context.socket(zmq.PUB)
            socket.connect('tcp://localhost:{0}'.format(port))

        return context, socket
