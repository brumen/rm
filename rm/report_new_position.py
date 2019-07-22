#! /usr/bin/python3

# reports new position when it is inserted into a database
#

import sys
import zmq
import time


def send_message( position
                , port = 5556
                , sleep_time = .2  ) -> None:
    """ Sends the position to the controller,
        acts as a publisher.

    :param position: position to message to the controller listening on port.
    :param port: port on which to send
    """

    socket  = zmq.Context().socket(zmq.PUB)
    socket.connect('tcp://localhost:{0}'.format(port))
    while True:
        socket.send_json({'event_type': 'delete_trade', 'trade_nb': 2})
        time.sleep(sleep_time)
        socket.send_json({'event_type': 'new_trade', 'trade_nb': 1})

# action to execute as a script
# send_message(str(sys.argv[1]))

send_message(111)