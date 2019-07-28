# main controlling logic for the risk management

import logging

from zmq     import Context as ZMQContext, PUB as ZMQ_PUB, SUB as ZMQ_SUB, SUBSCRIBE as ZMQ_SUBSCRIBE
from nanomsg import PUB as NANO_PUB, Socket as NanoSocket, SUB as NANO_SUB, PUB as NANO_PUB

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class ZMQSocketMixin:
    """ Socket Mixin.
    """

    @staticmethod
    def _create_socket(port, pub_sub='sub'):
        """ Create socket part.

        """

        context = ZMQContext()
        if pub_sub == 'sub':
            socket  = context.socket(ZMQ_SUB)
            socket.setsockopt_string(ZMQ_SUBSCRIBE, '')
            socket.bind("tcp://*:{0}".format(port))  # server ip
        else:
            socket = context.socket(ZMQ_PUB)
            socket.connect('tcp://localhost:{0}'.format(port))

        return context, socket


class NanoSocketMixin:

    @staticmethod
    def _create_socket(port, pub_sub='sub'):
        """ Create socket part.

        """

        if pub_sub == 'sub':
            socket = NanoSocket(NANO_SUB)
            socket.bind("tcp://*:{0}".format(port))
        else:
            socket = NanoSocket(NANO_PUB)
            socket.bind('tcp://localhost:{0}'.format(port))

        return None, socket
