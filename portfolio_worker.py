#
# a worker class that receives the portfolio and computes delta from it, returns the delta back to the controller.
#

import datetime
import logging
import time
import threading

from socket_msg     import NanoSocketMixin
from encode_decode  import EncodeDecodeMixin


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class PortfolioWorkerException(Exception):
    pass


class PortfolioWorker(EncodeDecodeMixin):

    def __init__( self
                , socket  : NanoSocketMixin.Socket  # PAIR recv, send_socket
                , mkt_date  = None
                , worker_name ='Gorazd'
                , sleep_time  = 0.0001
                , ):
        """ Worker process class.

        :param socket: PAIR nanomsg worker position_socket
        :param mkt_date: market date TODO: TO BE REMOVED LATER!!!
        :param worker_name: host name of the worker, used for identification.
        :param sleep_time: sleep time between iteration on the working thread.
        """

        self.socket        = socket
        self.mkt_date      = mkt_date
        self.__worker_name = worker_name
        self.__sleep_time  = sleep_time

        # signal handlers
        self.__is_revaluing_portfolio = False

    def is_working(self):
        return self.__is_revaluing_portfolio

    def start(self ):
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
            revalued_portfolio = self.__class__.revalue_portfolio(self._decode_message(msg_received), self.mkt_date)  # DeltaDict
            self.socket.send(self._encode_msg(revalued_portfolio))
            self.__is_revaluing_portfolio = False

            time.sleep(self.__sleep_time)

    @staticmethod
    def revalue_portfolio(portfolio, mkt_date : datetime.date):
        """ Revalues the portfolio given.

        :param portfolio: trade portfolio to use.
        :param mkt_date: market date
        :returns: value of the portfolio being processed.
        """

        raise PortfolioWorkerException('revalue_portfolio method not defined in the class')
