#
# a worker class that receives the portfolio and computes delta from it, returns the delta back to the controller.
#

import datetime
import logging
import time
import threading

from typing import Callable

from rm.socket_msg    import NanoSocketMixin, NNGSocketMixin
from rm.encode_decode import EncodeDecodeMixin


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class PortfolioWorkerException(Exception):
    pass


class PortfolioWorker(EncodeDecodeMixin):

    def __init__( self
                , socket            : NNGSocketMixin.Socket  # subtype of this actually.
                , revalue_portfolio : Callable
                , worker_name       : str           = 'Worker_1'
                , sleep_time        : float         = .0001
                , ):
        """ Worker process class.

        :param socket: NNG socket to use for communication - socket has to enable .recv() and .send() methods.
        :param worker_name: host name of the worker, used for identification.
        :param sleep_time: sleep time between iteration on the working thread.
        """

        self.socket             = socket
        self._revalue_portfolio = revalue_portfolio
        self.__worker_name      = worker_name
        self.__sleep_time       = sleep_time

        # signal handlers
        self.__is_revaluing_portfolio = False

    def is_working(self):
        return self.__is_revaluing_portfolio

    def start(self):
        """ Starts the worker, does the computation.
        """

        logger.info('Starting worker {0}'.format(self.__worker_name))

        threading.Thread(target=self.do_work).start()

    def do_work(self) -> None:
        """ Computes the incremental delta of the portfolio and sends it over the socket back to controller.
        """

        while True:
            msg_received = self.socket.recv()
            self.__is_revaluing_portfolio = True
            revalued_portfolio = self.__class__.revalue_portfolio(self._decode_message(msg_received))  # DeltaDict
            self.socket.send(self._encode_msg(revalued_portfolio))
            self.__is_revaluing_portfolio = False

            time.sleep(self.__sleep_time)
