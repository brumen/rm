#
# a worker class that receives the portfolio and computes delta from it, returns the delta back to the controller.
#

import logging

from typing import Callable

from rm.sockets.encode_decode import EncodeDecodeMixin
from rm.flow.in_out_updater import InOutUpdater

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class PortfolioWorkerException(Exception):
    pass


class PortfolioWorker(InOutUpdater, EncodeDecodeMixin):

    def __init__( self
                , value_portfolio_fct : Callable
                , worker_name         : str           = 'Worker_1'
                , sleep_time          : float         = .0001
                , ):
        """ Worker process class.

        :param worker_name: host name of the worker, used for identification.
        :param sleep_time: sleep time between iteration on the working thread.
        """

        self._worker_socket     = self.input('RandomInput', 'worker_socket')
        self._result_socket     = self.output('BaseOutput')
        self._value_portfolio_fct = value_portfolio_fct
        self.__worker_name      = worker_name
        self.__sleep_time       = sleep_time

        # signal handlers
        self.__is_revaluing_portfolio = False

    @property
    def is_working(self):
        return self.__is_revaluing_portfolio

    @is_working.setter
    def is_working(self, new_is_working):
        self.__is_revaluing_portfolio = new_is_working

    @property
    def is_available(self):
        return not self.is_working

    @property
    def worker_name(self):
        return self.__worker_name

    def transform(self):
        worker_name = self.worker_name
        self.is_working = True
        logger.info('Worker {0} works on the portfolio.'.format(worker_name))
        self._result_socket << self._value_portfolio_fct(self._worker_socket())
        logger.info('Worker {0} finished portfolio computations.'.format(worker_name))
        self.is_working = False
