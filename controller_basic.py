# main controlling logic for the risk management

import time
import logging

from threading import Thread
from typing    import List, Tuple, Callable, Union
from queue     import Queue

from rm.encode_decode    import EncodeDecodeMixin
from rm.portfolio_worker import PortfolioWorker
from rm.delta_dict       import DeltaDict


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class ControllerBase(EncodeDecodeMixin):
    """ Base controller class.
    """

    def __init__(self
                , queue_size          : int = 100000
                , value_portfolio_fct : Callable = None
                , workers             : Union[List[PortfolioWorker], None] = None
                , ):
        """ Controller class, keeps track of the system and distributes work.

        :param queue_size: maximum size of the queue.
        :param value_portfolio_fct: function computing the given portfolio.
        :param workers: workers associated w/ the controller.
        #:param worker_sockets: sockets to the workers to distribute work.
        #                       {'worker_name': worker_socket}
        """

        self.__work_queue = Queue(maxsize=queue_size)

        # signal handlers
        self.__value_portfolio_fct    = value_portfolio_fct
        self.__workers                = workers

        # states of this state machine:

    @property
    def workers(self):
        return self.__workers

    def nb_workers(self) -> int:
        """ Returns the number of workers associated w/ the controller.
        """
        return len(self.workers)

    def available_workers(self) -> List[PortfolioWorker]:
        """ Returns the workers which are available for work.
        """

        return [worker for worker in self.workers if worker.is_available() ]

    def add_to_queue(self, work : List):
        """ Adds values in the list work to the work_queue.

        :param work: work to add to the queue.
        :returns:
        """
        for value in work:
            self.__work_queue.put(value)

    def _distribute_workload(self, sleep_time = 0.0001) -> None:
        """ Distributes the workload to workers, looks into self.__work_queue and distributes this to the workers.
        """

        logger.info('Starting distribute_workload thread.')

        while True:
            if not self.__work_queue.empty():  # work to be done
                q_size = self.__work_queue.qsize()
                logger.debug('Distribute queue length: {0}'.format(q_size))

                workers_available = self.available_workers()
                logger.debug('Workers available: {0}'.format(workers_available))

                if workers_available:  # we have any workers
                    self.__schedule_work_to_workers()
                else:
                    logger.info('No workers available, queue size is {0}'.format(q_size))

            else:
                time.sleep(sleep_time)

    def __schedule_work_to_workers(self):
        """ Scheduling the work to workers. This is load-balancing part.

        :return:
        """

        work_queue = self.__work_queue
        work_queue_size = work_queue.qsize()
        work_to_distribute = [work_queue.get() for _ in range(work_queue_size)]
        workers_available = self.available_workers()
        nb_workers_available = len(workers_available)

        logger.info('Number of workers available: {0}'.format(nb_workers_available))

        for work_idx in range(work_queue_size):
            workers_available[work_idx % nb_workers_available].add_in_queue(work_to_distribute[work_idx])

    def __log_queue_length(self, sleep_time=.5):
        """ Only logs the length of positions still to process, for informative purposes.

        :param sleep_time: time to sleep between successive updated.
        :returns: logs the length of the queue if queue length > 50.
        """

        old_queue_size = 0

        while True:
            new_queue_size = self.__work_queue.qsize()

            if abs(new_queue_size - old_queue_size) > 50:
                old_queue_size = new_queue_size
                logger.info('Current positions queue length: {0}'.format(new_queue_size))

            logger.info('Queue length < 50')
            time.sleep(sleep_time)

    def start(self):
        """ Starts all the threads of the controller.
        """

        logger.info('Starting controller distribute workload thread.')
        Thread(target=self._distribute_workload).start()
        logger.info('Starting controller log queue thread.')
        Thread(target=self.__log_queue_length).start()  # logs the length of the portfolio queue still to process


class Controller2(ControllerBase):
    """ Controlling logic with sockets.
    """

    def __init__(self
                 , work_socket   = None
                 , result_socket = None
                 , queue_size          : int = 100000
                 , value_portfolio_fct : Callable = None
                 , workers             : Union[List[PortfolioWorker], None] = None
                 , ):
        """ Controller class, keeps track of the system and distributes work.

        :param work_socket: socket over which work is given.
        :param queue_size: maximum size of the queue.
        :param value_portfolio_fct: function computing the given portfolio.
        :param workers: workers associated w/ the controller.
        """

        super().__init__(queue_size = queue_size, value_portfolio_fct=value_portfolio_fct, workers=workers)

        self._work_socket   = work_socket
        self._result_socket = result_socket

        # results variable
        self.__is_working  = False
        self.__curr_result = None

    @property
    def is_working(self) -> bool:
        return self.__is_working

    @is_working.setter
    def is_working(self, new_is_revaluing : bool):
        self.__is_working = new_is_revaluing

    @property
    def curr_result(self) -> DeltaDict:
        """ Returns the current delta of the portfolio.
        """
        return self.__curr_result

    @curr_result.setter
    def curr_result(self, new_result : DeltaDict):
        self.__curr_result = new_result

    def _fill_work_queue(self):
        """ Looks on the worker socket and distributes the work.

        :return:
        """

        while True:
            self.add_to_queue(self._work_socket.recv())

    def _report_results(self, sleep_time = .5):
        """ Method should report the results.
        """

        # collect results from workers.


    def start(self):
        """ Starts all the threads of the controller.
        """

        super().start()  # start threads in the base class.

        Thread(target=self._fill_work_queue).start()
        Thread(target=self._check_replies_from_workers).start()
        Thread(target=self._report_results).start()
        Thread(target=self._handle_market_event).start()  # thread for handling market events.
