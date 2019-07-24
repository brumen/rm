import time
import zmq


class Server1:
    """ Main controlling logic.

    """

    def __init__(self
                , mkt_date = None
                , port     = 5556
                , queue_size = 1000):


        # zmq section of the controller
        self.port      = port
        self.__context = zmq.Context()
        self.__socket  = self.__context.socket(zmq.SUB)
        self.__socket.setsockopt_string(zmq.SUBSCRIBE, '')
        self.__socket.bind("tcp://*:{0}".format(self.port))  # server ip

    def start(self):
        """ Starts all the threads of the controller.

        """

        while True:
            msg = self.__socket.recv()
            print(msg)
            time.sleep(0.3)


if __name__ == '__main__':
    c1 = Server1(port=5555)
    c1.start()
