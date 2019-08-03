# main controlling logic for the risk management

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class ZMQSocketMixin:
    """ Socket Mixin.
    """

    from zmq import (Context, PUB, SUB, SUBSCRIBE)

    @staticmethod
    def _create_socket(port, pub_sub='sub'):
        """ Create socket part.
        """

        context = ZMQSocketMixin.Context()
        if pub_sub == 'sub':
            socket = context.socket(ZMQSocketMixin.SUB)
            socket.setsockopt_string(ZMQSocketMixin.SUBSCRIBE, '')
            socket.bind("tcp://*:{0}".format(port))  # server ip
        else:
            socket = context.socket(ZMQSocketMixin.PUB)
            socket.connect('tcp://localhost:{0}'.format(port))

        return context, socket


class NanoSocketMixin:

    from nanomsg import (PUB, Socket, SUB, PUB, SUB_SUBSCRIBE)

    @staticmethod
    def _create_socket( port
                      , pub_sub='sub'
                      , host = '127.0.0.1'):
        """ Create socket part.
        """

        if pub_sub == 'sub':
            socket = NanoSocketMixin.Socket(NanoSocketMixin.SUB)
            socket.connect("tcp://{0}:{1}".format(host, port))
            socket.set_string_option(NanoSocketMixin.SUB, NanoSocketMixin.SUB_SUBSCRIBE, '')
        else:
            socket = NanoSocketMixin.Socket(NanoSocketMixin.PUB)
            socket.bind('tcp://{0}:{1}'.format(host, port))

        return None, socket
