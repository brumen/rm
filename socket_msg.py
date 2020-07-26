# main controlling logic for the risk management

import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class NanoSocketMixin:

    from nanomsg import (PUB, Socket, SUB, SUB_SUBSCRIBE, PAIR)

    NANOMSG_TYPES = ['sub', 'pub', 'pair,send', 'pair,recv']  # allowed types for NanoSockets

    _TCP_STYLE = "tcp://{0}:{1}"

    @staticmethod
    def create_socket( port    : int
                     , pub_sub : str = 'sub'
                     , host    : str  = '127.0.0.1' ) -> Socket:
        """ Create position_socket part.

        :param port: which port to use for the socket.
        :param pub_sub: type of subscription to use, default subscription.
        :param host: host where the socket should look at, default localhost
        :returns: nanomsg position_socket
        """

        address = NanoSocketMixin._TCP_STYLE.format(host, port)

        assert pub_sub in NanoSocketMixin.NANOMSG_TYPES,\
            'pub_sub parameter {0} not one of {1}'.format(pub_sub, NanoSocketMixin.NANOMSG_TYPES)

        if pub_sub == 'sub':
            socket = NanoSocketMixin.Socket(NanoSocketMixin.SUB)
            socket.connect(address)
            socket.set_string_option(NanoSocketMixin.SUB, NanoSocketMixin.SUB_SUBSCRIBE, '')
            return socket

        if pub_sub == 'pub':
            socket = NanoSocketMixin.Socket(NanoSocketMixin.PUB)
            socket.bind(address)
            return socket

        if pub_sub == 'pair,send':  # send part of part
            socket = NanoSocketMixin.Socket(NanoSocketMixin.PAIR)
            socket.bind(address)
            return socket

        if pub_sub == 'pair,recv':  # receiver part of pair
            socket = NanoSocketMixin.Socket(NanoSocketMixin.PAIR)
            socket.connect(address)
            return socket


# TODO: THIS NEEDS SOME WORK - SUBSCRIBERS HAVE TOPICS ETC
class NNGSocketMixin:
    """ Nano sockets next generation, an improvement for Nano messages.
    """

    from pynng import (Pub0, Socket, Sub0, Pair0)

    NANOMSG_TYPES = NanoSocketMixin.NANOMSG_TYPES

    _TCP_STYLE = NanoSocketMixin._TCP_STYLE

    @staticmethod
    def _create_socket( port
                      , pub_sub = 'sub'
                      , host    = '127.0.0.1') -> Socket:
        """ Create position_socket part.

        :returns: nanomsg position_socket
        """

        address = NNGSocketMixin._TCP_STYLE.format(host, port)

        assert pub_sub in NNGSocketMixin.NANOMSG_TYPES,\
            'pub_sub parameter {0} not one of {1}'.format(pub_sub, NNGSocketMixin.NANOMSG_TYPES)

        if pub_sub == 'sub':
            return NNGSocketMixin.Sub0(dial=address)
            # socket.set_string_option(NanoSocketMixin.SUB, NanoSocketMixin.SUB_SUBSCRIBE, '')

        if pub_sub == 'pub':
            return NNGSocketMixin.Pub0(listen=address)

        if pub_sub == 'pair,send':  # send part of part
            return NNGSocketMixin.Pair0(dial=address)

        if pub_sub == 'pair,recv':  # receiver part of pair
            return NNGSocketMixin.Pair0(listen=address)
