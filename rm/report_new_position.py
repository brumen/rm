#! /usr/bin/python3

# reports new position when it is inserted into a database
#

import zmq


def send_message( position
                , port = 5556 ) -> None:
    """ Sends the position to the controller.

    :param position:
    :param port: port on which to send
    """

    context = zmq.Context()
    socket  = context.socket(zmq.PAIR)
    socket.bind("tcp://*:{0}".format(port))

    socket.send_json(position)


# action to execute
argv = 1
argc = 2
send_message(argv)
