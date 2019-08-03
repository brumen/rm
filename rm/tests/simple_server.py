import time
from socket_msg import NanoSocketMixin


class Server1:
    """ Main controlling logic.

    """

    def __init__(self
                , socket ):

        self.__socket = socket

    def start(self):
        """ Starts all the threads of the controller.

        """

        while True:
            msg = self.__socket.recv()
            print(msg)
            # time.sleep(0.3)


if __name__ == '__main__':
    c1 = Server1(NanoSocketMixin._create_socket(port=5556, pub_sub = 'sub')[1])
    c1.start()
