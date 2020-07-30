# main controlling logic for the risk management

import time
import logging

from threading import Thread
from typing    import List, Tuple, Callable, Union
from queue     import Queue

from rm.delta_dict       import DeltaDict
from rm.encode_decode    import EncodeDecodeMixin
from rm.portfolio_worker import PortfolioWorker

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class Controller(EncodeDecodeMixin):
    """ Controlling logic of the position updater.
    """

    def __init__(self
                 , position_socket
                 , market_socket       = None
                 , queue_size          : int = 100000
                 , value_portfolio_fct : Callable = None
                 , workers             : Union[List[PortfolioWorker], None] = None
                 , query_socket        = None
                 , ):
        """ Controller class, keeps track of the system and distributes work.

        :param position_socket: socket over which new positions are obtained.
        :param market_socket: socket over which market updates are received.
        :param queue_size: maximum size of the queue.
        :param value_portfolio_fct: function computing the given portfolio.
        :param workers: workers associated w/ the controller.
        #:param worker_sockets: sockets to the workers to distribute work.
        #                       {'worker_name': worker_socket}
        :param query_socket: sockets where one can subscribe to and query for results.
        """

        self.__position_socket     = position_socket
        self.__market_socket       = market_socket
        self.__new_position_queue  = Queue(maxsize=queue_size)

        # signal handlers
        self.__is_revaluing_portfolio = False
        self._portfolio               = []  # initially empty portfolio
        self.__value_portfolio_fct    = value_portfolio_fct
        # self.__worker_sockets         = worker_sockets
        self.__workers                = workers
        self.__query_socket           = query_socket

        # states of this state machine:
        # worker_available is a list of True/False depending if these workers are available or not. List[bool]
        nb_workers = len(workers)
        self.__worker_available = [True] * nb_workers if workers else None  # all workers are available
        self.__worker_queues    = [Queue(maxsize=5)] * self.__nb_workers
        self.__trades_on_most_recent_market = []  # no trades processed yet.
        self.__revaluing_market_event = False
        self.__latest_market_update = None

        # results variable
        self.__curr_result = DeltaDict({})

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

        return [worker for worker in self.workers if not worker.is_working() ]

    def _market_event(self):
        """ Returns the market event from the market socket.

        :returns: market message
        """

        return self.__market_socket.recv()

    @property
    def is_revaluing_portfolio(self) -> bool:
        return self.__is_revaluing_portfolio

    @is_revaluing_portfolio.setter
    def is_revaluing_portfolio(self, new_is_revaluing : bool):
        self.__is_revaluing_portfolio = new_is_revaluing

    @property
    def curr_portfolio(self) -> List[Tuple]:
        """ Returns the current portfolio under consideration.

        :returns: list of individual trades.
        """

        return self._portfolio

    @curr_portfolio.setter
    def curr_portfolio(self, new_portfolio):
        self._portfolio = new_portfolio

    @property
    def curr_result(self) -> DeltaDict:
        """ Returns the current delta of the portfolio.
        """

        return self.__curr_result

    @curr_result.setter
    def curr_result(self, new_result : DeltaDict):
        self.__curr_result = new_result

    def _trades_on_most_recent_market(self):
        """ Display the trade portfolio on the most recent market.
        """
        pass

    def _get_trade_params(self, position_id : int) -> List[Tuple]:
        """ Gets the trade parameters to dispatch to the workers.

        :param position_id: trade id position that we want to fetch.
        :returns: list of parameters for the position_id.
        """

        raise NotImplementedError('Class should implement _get_trade_params.')

    # TODO: CHECK IF THIS IS NECESSARY!!!
    def _new_position_event(self, new_position_l : List, trade_type='new_trade') -> None:
        """ Update the state 'What to do when a new position comes in'.

        :param new_position_l: position list of new trades.
        :param trade_type: type of trade amendment ('new_trade', 'delete_trade')
        :returns: None, performs the trade augmentation & delta recomputation.
        """

        self.curr_portfolio.extend(new_position_l)
        delta_difference = self.__value_portfolio_fct(new_position_l)

        self.curr_result += delta_difference if trade_type == 'new_trade' else self.curr_result - delta_difference

    def _fill_event_queue(self, sleep_time = .0001):
        """ Fills the self.__work_queue with events coming from the position updater.
        """

        logger.debug('Starting the event queue thread.')

        while True:
            self.__new_position_queue.put(self.__position_socket.recv())
            time.sleep(sleep_time)

    def _fill_worker_queue(self, worker : PortfolioWorker, sleep_time=.0001):
        """ Fills the worker queue with the results of worker computation.

        :param worker: worker whose work needs to be added.
        :param sleep_time: sleep time for the thread.
        """

        logger.info('Starting fill worker queue thread.')

        while True:
            worker.add_in_queue(TODO)  # TODO: WHAT TO PUT THERE .put(self.__worker_sockets[worker_idx].recv())
            time.sleep(sleep_time)

    def _check_replies_from_workers(self):
        """ Checks the replies from workers, and potentially update self.curr_delta.
        """

        logger.info('Starting check_replies_from_workers thread.')

        while True:
            for worker_idx, worker_socket in enumerate(self.__worker_sockets):  # check sockets
                worker_queue_curr = self.__worker_queues[worker_idx]
                if not worker_queue_curr.empty():
                    self.curr_result += self._decode_message(worker_queue_curr.get())
                    self.__worker_available[worker_idx] = True

    def _handle_market_event(self):
        """ Handles the market events

        """

        # if the portfolio is revaluing, ignore the market event
        if not self.is_revaluing_portfolio and self._market_event():
            self.is_revaluing_portfolio = True
            self.curr_result = self.__value_portfolio_fct(self.curr_portfolio)  # produce the result
            self.is_revaluing_portfolio = False  # finished revaluing

        else:
            logger.info('Obtained market event {0}, dropping, previous market event not yet processed.')

    def _distribute_workload(self, sleep_time = 0.0001) -> None:
        """ Distributes the workload to workers, looks into self.__work_queue and distributes this to the workers.
        """

        logger.info('Starting distribute_workload thread.')

        while True:
            if not self.__new_position_queue.empty():  # work to be done
                q_size = self.__new_position_queue.qsize()
                logger.debug('Distribute queue length: {0}'.format(q_size))

                workers_available = self.available_workers()
                logger.debug('Workers available: {0}'.format(workers_available))

                if workers_available:  # we have any workers
                    self.__schedule_work_to_workers(q_size)
                else:
                    logger.info('No workers available, queue size is {0}'.format(q_size))

            time.sleep(sleep_time)

    def __schedule_work_to_workers( self
                                  , work_to_distribute : List ):
        """ Scheduling the work to workers. This is load-balancing part.

        :param work_to_distribute: list of work to be processed by the workers.
        :return:
        """

        workers_available = self.available_workers()
        nb_workers_available = len(workers_available)
        nb_work              = len(work_to_distribute)
        # work_per_worker      = nb_work // nb_workers_available
        logger.info('Number of workers available: {0}'.format(nb_workers_available))

        for work_idx in range(nb_work):
            workers_available[work_idx % nb_workers_available].add_in_queue(work_to_distribute[work_idx])
            # self.__worker_sockets[worker_to_select].send(self._encode_msg(self._get_trade_params(self._decode_message(self.__work_queue.get())['trade_nb'])))
            # self.__worker_available[worker_to_select] = False

    def __get_positions_from_queue(self, nb_messages : int) -> List:
        """ Takes a number of messages from the queue and prepares them to be sent to the workers.

        :param nb_messages: number of messages to take from the queue.
        :returns: TODO
        """

        logger.info('Nb. new positions: {0}'.format(nb_messages))

        return [ self._get_trade_params(self._decode_message(self.__new_position_queue.get())['trade_nb'])[0]
                 for _ in range(nb_messages) ]

    def _report_results(self, sleep_time = .5):
        """ Method should report the results
        """

        raise NotImplementedError('Not implemented method _report_results')

    def __log_queue_length(self, sleep_time=.5):
        """ Only logs the length of positions still to process, for informative purposes.

        :param sleep_time: time to sleep between successive updated.
        :returns: logs the length of the queue if queue length > 50.
        """

        old_queue_size = 0

        while True:
            new_queue_size = self.__new_position_queue.qsize()

            if abs(new_queue_size - old_queue_size) > 50:
                old_queue_size = new_queue_size
                logger.info('Current positions queue length: {0}'.format(new_queue_size))

            logger.info('Queue length < 50')
            time.sleep(sleep_time)

    def start(self):
        """ Starts all the threads of the controller.
        """

        logger.info('Starting controller threads.')

        # threads that are started
        Thread(target=self._fill_event_queue).start()
        for worker_idx in range(self.nb_workers()):  # fill worker threads
            Thread(target=lambda : self._fill_worker_queue(worker_idx)).start()
        Thread(target=self._check_replies_from_workers).start()
        Thread(target=self._distribute_workload).start()
        Thread(target=self._report_results).start()
        Thread(target=self.__log_queue_length).start()  # logs the length of the portfolio queue still to process
        Thread(target=self._handle_market_event).start()  # thread for handling market events.
