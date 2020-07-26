#! /usr/bin/python3

# reports new position when it is inserted into a database
#

import time

from socket_msg import NanoSocketMixin


def send_message( position : str
                , port = 5556
                , sleep_time = .3  ) -> None:
    """ Sends the position to the controller,
        acts as a publisher.

    :param position: position to message to the controller listening on port.
    :param port: port on which to send
    """

    _, socket = NanoSocketMixin._create_socket(5556, pub_sub='pub')
    time.sleep(.09)  # this has to be here!!!
    socket.send(position)
    socket.close()

# action to execute as a script
# send_message(str(sys.argv[1]))

send_message('111')
