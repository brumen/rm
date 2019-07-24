#! /usr/bin/python3

# reports new position when it is inserted into a database
#

import sys
import zmq
import time


def send_message( position
                , port = 5556
                , sleep_time = .3  ) -> None:
    """ Sends the position to the controller,
        acts as a publisher.

    :param position: position to message to the controller listening on port.
    :param port: port on which to send
    """

    return position

# action to execute as a script
# send_message(str(sys.argv[1]))

send_message(111)
