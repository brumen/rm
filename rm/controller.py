# main controlling logic for the risk management

import zmq
from ao.mysql

class Controller:
    """ Main controlling logic.

    """

    def __init__(self
                , port = 5556 ):

        # zmq section of the controller
        self.port      = port
        self.__context = zmq.Context()
        self.__socket  = self.__context.socket(zmq.PAIR)
        self.__socket.bind("tcp://*:{0}".format(self.port))

        # signal handlers
        self.__is_revaluing_portfolio = False

        self.__portfolio = []  # no portfolio

    def start(self):
        """ Start the controller.

        """
        pass

    def _read_portfolio(self):
        """

        :return:
        """

    def _new_market_event(self):
        """ What to do when a new market event occurs.

        :return:
        """


    def _new_position_event(self):
        """ What to do when a new position comes in.

        :return:
        """

        pass

    def _revalue_portfolio(self):
        """ Revalue the entire portfolio.

        :return:
        """

        self.__is_revaluing_portfolio = True

        # TODO: SOMETHING

        self.__is_revaluing_portfolio = False
