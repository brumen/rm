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
        """ Create position_socket part.
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

    from nanomsg import (PUB, Socket, SUB, PUB, SUB_SUBSCRIBE, PAIR)

    NANOMSG_TYPES = ['sub', 'pub', 'pair,send', 'pair,recv']  # allowed types for NanoSockets

    _TCP_STYLE = "tcp://{0}:{1}"

    @staticmethod
    def _create_socket( port
                      , pub_sub = 'sub'
                      , host    = '127.0.0.1') -> Socket:
        """ Create position_socket part.

        :returns: nanomsg position_socket
        """

        assert pub_sub in NanoSocketMixin.NANOMSG_TYPES,\
            'pub_sub parameter {0} not one of {1}'.format(pub_sub, NanoSocketMixin.NANOMSG_TYPES)

        if pub_sub == 'sub':
            socket = NanoSocketMixin.Socket(NanoSocketMixin.SUB)
            socket.connect(NanoSocketMixin._TCP_STYLE.format(host, port))
            socket.set_string_option(NanoSocketMixin.SUB, NanoSocketMixin.SUB_SUBSCRIBE, '')
            return socket

        if pub_sub == 'pub':
            socket = NanoSocketMixin.Socket(NanoSocketMixin.PUB)
            socket.bind(NanoSocketMixin._TCP_STYLE.format(host, port))
            return socket

        if pub_sub == 'pair,send':  # send part of part
            socket = NanoSocketMixin.Socket(NanoSocketMixin.PAIR)
            socket.bind(NanoSocketMixin._TCP_STYLE.format(host, port))
            return socket

        if pub_sub == 'pair,recv':  # receiver part of pair
            socket = NanoSocketMixin.Socket(NanoSocketMixin.PAIR)
            socket.connect(NanoSocketMixin._TCP_STYLE.format(host, port))
            return socket
