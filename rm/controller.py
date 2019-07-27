# main controlling logic for the risk management

import time
import datetime
import sys
import json
import logging
import threading
sys.path.append('/home/brumen/work/rm/ao/')

from typing import Dict, List, Tuple

from queue import Queue

from delta_dict              import DeltaDict
from socket                 import SocketMixin

from ao.mysql_connector_env import MysqlConnectorEnv
from ao.air_option          import AirOptionMock

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class Controller(SocketMixin):
    """ Main controlling logic.

    """

    def __init__(self
                , mkt_date = None
                , port     = 5556
                , queue_size = 1000):

        self.mkt_date = mkt_date if mkt_date else datetime.date.today()  # market date is today or provided date

        self.__msg_queue = Queue(maxsize=queue_size)

        # zmq section of the controller
        self.port = port
        self__context, self.__socket = self._create_socket(port, pub_sub='sub')

        # signal handlers
        self.__is_revaluing_portfolio = False
        self.__curr_delta = DeltaDict({})
        self.__portfolio  = []  # initially empty portfolio

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

    def _fill_queue(self, sleep_time = .1 ):
        """ function to fill the queue w/ messages.

        """

        while True:
            msg_received = self.__socket.recv()
            self.__msg_queue.put(json.loads(msg_received.decode('utf-8')))
            time.sleep(sleep_time)

    def _process_queue(self, sleep_time = .1):
        """ Handles the self.__msg_queue queue processing - if not empty, process messages, else

        :return:
        """

        while True:
            if not self.__msg_queue.empty():  # work to be done
                msg_to_process = self.__msg_queue.get()
                self._handle_msg(msg_to_process)
                time.sleep(sleep_time)

    def start(self):
        """ Starts all the threads of the controller.

        """

        logger.info('Starting controller.')
        fill_queue_thread    = threading.Thread(target=self._fill_queue)
        process_queue_thread = threading.Thread(target=self._process_queue)

        fill_queue_thread.start()
        process_queue_thread.start()

    def __get_trade_params(self, position_id : int) -> List[Tuple]:
        """ Get trade params for trade under position_id in the db.

        :param position_id: position id of the trade considered.
        :returns: list of tuples for position_id
        """

        with MysqlConnectorEnv(host='localhost') as db_conn:  # TODO FIX HERE
            cursor = db_conn.cursor()
            cursor.execute('SELECT * FROM option_positions WHERE position_id = {0}'.format(position_id))
            return cursor.fetchall()

    def _handle_msg(self, msg : Dict) -> None:
        """ Handle the message received.

        :param msg: dictionary containing the message received.
        :returns: updates trade positions &
        """

        self._new_position_event( self.__get_trade_params(msg['trade_nb'])
                                , msg['event_type'])

    def _read_portfolio(self, db_host='localhost'):
        """ Reads the entire portfolio from the database.

        :returns:
        """

        with MysqlConnectorEnv(host=db_host) as db_conn:
            return db_conn.cursor().execute('SELECT * FROM options_positions').fetchall()

    def _new_market_event(self):
        """ What to do when a new market event occurs.

        :return:
        """

        self.__is_revaluing_portfolio = True
        self.curr_delta = Controller.__revalue_portfolio(self.curr_portfolio
                                                         , self.mkt_date)
        self.__is_revaluing_portfolio = False

    def _new_position_event(self, new_position_l : List, trade_type='new_trade') -> None:
        """ Update the state What to do when a new position comes in.

        :param new_position_l: position list of new trades.
        :param trade_type: type of trade amendment ('new_trade', 'delete_trade')
        :returns: None, performs the trade augmentation & delta recomputation.
        """

        self.__portfolio.extend(new_position_l)
        delta_difference = Controller.__revalue_portfolio(new_position_l, self.mkt_date)

        self.curr_delta = self.curr_delta + delta_difference if trade_type == 'new_trade' else self.curr_delta - delta_difference

    def _revalue_current_portfolio(self):
        """ Revalue the entire portfolio.

        :return:
        """

        self.__is_revaluing_portfolio = True
        self.curr_delta = Controller.__revalue_portfolio(self.curr_portfolio, self.mkt_date)
        self.__is_revaluing_portfolio = False

    @staticmethod
    def __revalue_portfolio(portfolio, mkt_date : datetime.date) -> DeltaDict:
        """ Revalues the portfolio given.

        :param portfolio: trade portfolio to use.
        :param mkt_date: market date
        :return:
        """

        logger.info('Computing portfolio {0}'.format(str(portfolio)))
        portfolio_delta = DeltaDict({})

        for _, orig, dest\
             , option_start_date\
             , option_end_date\
             , option_ret_start_date\
             , option_ret_end_date\
             , outbound_date_start\
             , outbound_date_end\
             , inbound_date_start\
             , inbound_date_end\
             , K\
             , carrier\
             , adults\
             , cabinclass in portfolio:

            # computing pv01 - delta
            portfolio_delta += AirOptionMock( mkt_date
                                    , orig
                                    , dest
                                    , option_start_date = option_start_date
                                    , option_end_date   = option_end_date
                                    , option_ret_start_date = option_ret_start_date
                                    , option_ret_end_date   = option_ret_end_date
                                    , outbound_date_start   = outbound_date_start
                                    , outbound_date_end     = outbound_date_end
                                    , inbound_date_start    = inbound_date_start
                                    , inbound_date_end      = inbound_date_end
                                    , K                     = K
                                    , carrier               = carrier
                                    , adults                = adults
                                    , cabinclass            = cabinclass ).PV01()

        return portfolio_delta


if __name__ == '__main__':
    c1 = Controller()
    c1.start()
