#! /usr/bin/python3

# reports new position when it is inserted into a database
#

import sys
import zmq
import time


def send_message( position
                , port = 5556 ) -> None:
    """ Sends the position to the controller.

    :param position: position to message to the controller listening on port.
    :param port: port on which to send
    """

    socket  = zmq.Context().socket(zmq.REQ)
    socket.connect('tcp://localhost:{0}'.format(port))
    socket.send_json(position)


send_message({'event_type': 'delete_trade', 'trade_nb': 2})
# time.sleep(2)
#send_message({'event_type': 'new_trade', 'trade_nb': 1})

# action to execute as a script
# send_message(str(sys.argv[1]))
