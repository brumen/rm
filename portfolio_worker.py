#
# a worker class that receives the portfolio and computes delta from it, returns the delta back to the controller.
#

import logging
import threading

from typing import Callable, List
from queue  import Queue

from rm.encode_decode import EncodeDecodeMixin
from rm.in_out_updater import InOutUpdater

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
                , max_queue_size      : int           = 5000
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
        self.__max_queue_size   = max_queue_size

        # signal handlers
        self.__is_revaluing_portfolio = False
        self.__worker_queue = Queue(maxsize = max_queue_size)

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

    def start(self):
        """ Starts the worker, does the computation.
        """

        logger.info('Starting worker {0}'.format(self.__worker_name))

        threading.Thread(target=self.do_work).start()

    def add_in_queue(self, values : List) -> None:
        """ Adds the values in the queue for the worker to process.

        :return:
        """

        for value in values:
            self.__worker_queue.put(value)

    def _publish_results(self, result):
        raise NotImplementedError('TODO: FINISH THIS HERE')

    def _revalue_portfolio(self, work : List) -> List:
        """ Returns the reavalued portfolio.

        :param work:
        :return:
        """

        return self._value_portfolio_fct(work)

    def transform(self):
        worker_name = self.worker_name
        self.is_working = True
        logger.info('Worker {0} works on the portfolio.'.format(worker_name))
        self._result_socket << self._revalue_portfolio(self._worker_socket())
        logger.info('Worker {0} finished portfolio computations.'.format(worker_name))
        self.is_working = False
