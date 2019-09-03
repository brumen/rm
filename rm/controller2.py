# main controlling logic for the risk management

import time
import datetime
import sys
import logging
import threading
sys.path.append('/home/brumen/work/rm/ao/')

from typing import Dict, List, Tuple
from queue  import Queue

from delta_dict             import DeltaDict
from socket_msg             import NanoSocketMixin
from encode_decode          import EncodeDecodeMixin

from ao.mysql_connector_env import MysqlConnectorEnv
from portfolio_worker       import PortfolioAirWorker

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class Controller(EncodeDecodeMixin):
    """ Controlling logic of the position updater.
    """

    def __init__(self
                 , position_socket
                 , position_db_address ='127.0.0.1'
                 , mkt_date = None
                 , queue_size = 100000
                 , revalue_portfolio = PortfolioAirWorker.revalue_portfolio
                 , worker_sockets = None
                 ):
        """ Controller class, keeps track of the system and distributes work.

        :param position_socket: socket over which new positions are obtained.
        :param position_db_address: database host where the position are read from
        :param mkt_date: market date (datetime.date), if None, revert to today
        :param queue_size: maximum size of the queue.
        :param revalue_portfolio: function computing the portfolio given.
        :param worker_sockets: sockets to the workers to distribute work.
                               {'worker_name': worker_socket}
        """

        self.__position_socket  = position_socket
        self.__position_db_address = position_db_address
        self.mkt_date = mkt_date if mkt_date else datetime.date.today()  # market date is today or provided date

        self.__new_position_queue = Queue(maxsize=queue_size)

        # signal handlers
        self.__is_revaluing_portfolio = False
        self.__curr_delta = DeltaDict({})
        self.__portfolio  = []  # initially empty portfolio
        self.__revalue_portfolio = revalue_portfolio
        self.__worker_sockets = worker_sockets

        # states of this state machine:
        self.__nb_workers = len(self.__worker_sockets)
        self.__worker_available = [True] * self.__nb_workers if self.__worker_sockets else None  # all workers are available
        self.__worker_queues    = [Queue(maxsize=5)] * self.__nb_workers

    def __worker_name(self, worker_idx):
        return 'Worker{0}'.format(worker_idx)

    @property
    def curr_delta(self) -> DeltaDict:
        return self.__curr_delta

    @curr_delta.setter
    def curr_delta(self, new_delta : DeltaDict):
        self.__curr_delta = new_delta

    @property
    def curr_portfolio(self) -> List[Tuple]:
        return self.__portfolio

    @curr_portfolio.setter
    def curr_portfolio(self, new_portfolio):
        self.__portfolio = new_portfolio

    def __get_trade_params(self, position_id : int) -> List[Tuple]:
        """ Get trade params for trade under position_id in the self.__position_db_address mysql db.

        :param position_id: position id of the trade considered.
        :returns: list of tuples for position_id
        """

        with MysqlConnectorEnv(host=self.__position_db_address) as db_conn:
            cursor = db_conn.cursor()
            # TODO: THIS CAN BE OPTIMIZED TO ACCEPT position_id lists
            cursor.execute('SELECT * FROM option_positions WHERE position_id = {0}'.format(position_id))
            return cursor.fetchall()

    def _new_position_event(self, new_position_l : List, trade_type='new_trade') -> None:
        """ Update the state 'What to do when a new position comes in'.

        :param new_position_l: position list of new trades.
        :param trade_type: type of trade amendment ('new_trade', 'delete_trade')
        :returns: None, performs the trade augmentation & delta recomputation.
        """

        self.__portfolio.extend(new_position_l)
        delta_difference = self.__revalue_portfolio(new_position_l, self.mkt_date)
        self.curr_delta += delta_difference if trade_type == 'new_trade' else self.curr_delta - delta_difference

    def _fill_event_queue(self, sleep_time = .0001):
        """ Fills the self.__new_position_queue with events coming from the position updater.
        """

        logger.debug('Starting the event queue thread.')
        queue_size = 0
        while True:
            new_queue_size = self.__new_position_queue.qsize()
            if abs(new_queue_size - queue_size) > 50:
                logger.info('Controller queue size: {0}.'.format(new_queue_size))
                queue_size = new_queue_size
            self.__new_position_queue.put(self.__position_socket.recv())
            time.sleep(sleep_time)

    def _fill_worker_queue(self, worker_idx, sleep_time=.0001):
        """ Fills the worker queue with the results of worker computation.

        :param worker_idx: index of the worker
        :param sleep_time:
        :return:
        """

        logger.info('Starting fill worker queue thread.')

        while True:
            self.__worker_queues[worker_idx].put(self.__worker_sockets[worker_idx].recv())
            time.sleep(sleep_time)

    def _check_replies_from_workers(self):
        """ Checks the replies from workers, and potentially update self.curr_delta.
        """

        logger.info('Starting check_replies_from_workers thread.')

        while True:
            for worker_idx, worker_socket in enumerate(self.__worker_sockets):  # check sockets
                worker_queue_curr = self.__worker_queues[worker_idx]
                if not worker_queue_curr.empty():
                    self.curr_delta += self._decode_message(worker_queue_curr.get())
                    self.__worker_available[worker_idx] = True

    def _distribute_workload(self, sleep_time=0.0001) -> None:
        """ Distributes the workload to workers, looks into self.__new_position_queue and distributes this to the workers.
        """

        logger.info('Starting distribute_workload thread.')

        while True:
            if not self.__new_position_queue.empty():  # work to be done
                q_size = self.__new_position_queue.qsize()
                logger.debug('Queue length: {0}'.format(q_size))
                workers_available = [ worker_idx for worker_idx, worker_available in enumerate(self.__worker_available)
                                      if worker_available ]
                logger.debug('Workers avail: {0}'.format(workers_available))
                if workers_available:  # we have any workers
                    self.__schedule_work_to_workers_2(workers_available, q_size)
            time.sleep(sleep_time)

    def __schedule_work_to_workers_2(self, workers_available, q_size):
        """ Improved version w/ workers. Above version SUCKS.

        :param workers_available: list of workers available for work.
        :param q_size:
        :return:
        """
        nb_workers_available = len(workers_available)
        logger.info('NB workers avail: {0}'.format(nb_workers_available))
        for msg_idx in range(q_size):
            worker_to_select = workers_available[msg_idx % nb_workers_available]
            self.__worker_sockets[worker_to_select].send(self._encode_msg(self.__get_trade_params(self._decode_message(self.__new_position_queue.get())['trade_nb'])))
            self.__worker_available[worker_to_select] = False

    def __schedule_work_to_workers(self, workers_available, q_size):
        """ Another scheduling mechanism.

        :param workers_available:
        :param q_size:
        :return:
        """

        nb_workers_available = len(workers_available)
        logger.info('NB workers avail: {0}'.format(nb_workers_available))
        for worker_idx in workers_available:
            self.__worker_sockets[worker_idx].send(self._encode_msg(self.__get_positions_from_queue(q_size // nb_workers_available)))
            self.__worker_available[worker_idx] = False

    def __get_positions_from_queue(self, nb_messages : int) -> List:
        """ Takes a number of messages from the queue and prepares them to be sent to the workers.

        :param nb_messages: number of messages to take from the queue.
        :returns: TODO
        """

        logger.info('NB MSG: {0}'.format(nb_messages))
        return [ self.__get_trade_params(self._decode_message(self.__new_position_queue.get())['trade_nb'])[0]
                 for _ in range(nb_messages) ]

    def __report_current_delta(self, sleep_time=.5):
        """ Reporting thread.
        """

        while True:
            logger.info('Current delta: {0}'.format(str(self.curr_delta)))
            time.sleep(sleep_time)

    def start(self):
        """ Starts all the threads of the controller.
        """

        logger.info('Starting controller threads.')

        # threads that are started
        threading.Thread(target=self._fill_event_queue).start()
        for worker_idx in range(self.__nb_workers):  # fill worker threads
            threading.Thread(target=lambda : self._fill_worker_queue(worker_idx)).start()
        threading.Thread(target=self._check_replies_from_workers).start()
        threading.Thread(target=self._distribute_workload).start()
        threading.Thread(target=self.__report_current_delta).start()


def start_workers(mkt_date : datetime.date, worker_ports : List[int]) -> List[PortfolioAirWorker] :
    """ Sets the workers and starts their .start function.

    :param mkt_date: market date
    :param worker_ports: number of workers to start
    """

    workers = []
    for worker_idx, worker_port in enumerate(worker_ports):
        curr_worker = PortfolioAirWorker(NanoSocketMixin._create_socket(port=worker_port, pub_sub='pair,recv')
                                         , mkt_date=mkt_date
                                         , worker_name='Worker{0}'.format(worker_idx))
        curr_worker.start()
        workers.append(curr_worker)

    return workers


if __name__ == '__main__':
    # nano_controller = Controller(NanoSocketMixin._create_socket(port=5556))  # to run as single server configuration

    # 3 workers configuration
    ports = [5667, 5668, 5669]
    workers = start_workers(datetime.date(2019, 9, 1), ports)  # on separate threads
    nano_controller = Controller( NanoSocketMixin._create_socket(port=5556)
                                , worker_sockets= [NanoSocketMixin._create_socket(port=port, pub_sub='pair,send') for port in ports] )

    nano_controller.start()
