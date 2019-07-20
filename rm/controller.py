# main controlling logic for the risk management

import time
import datetime
import zmq
import sys
sys.path.append('/home/brumen/work/rm/ao/')

from typing import Dict, List

from ao.mysql_connector_env import MysqlConnectorEnv
from ao.air_option          import AirOptionMock


class Controller:
    """ Main controlling logic.

    """

    def __init__(self
                , mkt_date = None
                , port     = 5556 ):

        self._mkt_date = mkt_date if mkt_date else datetime.date.today()  # market date is today or provided date

        # zmq section of the controller
        self.port      = port
        self.__context = zmq.Context()
        self.__socket  = self.__context.socket(zmq.PAIR)
        self.__socket.bind("tcp://*:{0}".format(self.port))  # server ip

        # signal handlers
        self.__is_revaluing_portfolio = False
        self.__curr_delta = None

        self.__portfolio = []  # no portfolio

    @property
    def curr_delta(self):
        return self.__curr_delta

    @curr_delta.setter
    def curr_delta(self, new_delta):
        self.__curr_delta = new_delta

    @property
    def curr_portfolio(self):
        return self.__portfolio

    @curr_portfolio.setter
    def curr_portfolio(self, new_portfolio):
        self.__portfolio = new_portfolio

    def start(self
             , sleep_time = .1 ):
        """ Starts the controller.

        """

        while True:
            msg_received = self.__socket.recv()
            print(msg_received)
            time.sleep(sleep_time)  # sleep .1 seconds

    def _read_portfolio(self, db_host='localhost'):
        """ Reads the entire portfolio from the database.

        :returns:
        """

        with MysqlConnectorEnv(host=db_host) as db_conn:
            self.curr_portfolio = db_conn.cursor().execute('SELECT * FROM options_positions').fetchall()

        return self.curr_portfolio

    def _new_market_event(self):
        """ What to do when a new market event occurs.

        :return:
        """

        self.__is_revaluing_portfolio = True
        self.curr_delta = Controller.__revalue_portfolio( self.curr_portfolio
                                                        , self._mkt_date)
        self.__is_revaluing_portfolio = False

    def _new_position_event(self, new_position_l : List) -> None:
        """ Update the state What to do when a new position comes in.

        :returns:
        """

        self.__portfolio.extend(new_position_l)

        self.curr_delta = Controller.__merge_deltas( self.curr_delta
                                                   , Controller.__revalue_portfolio( new_position_l
                                                                                   , self._mkt_date)
                                                   )

    def _revalue_current_portfolio(self):
        """ Revalue the entire portfolio.

        :return:
        """

        self.__is_revaluing_portfolio = True
        self.curr_delta = Controller.__revalue_portfolio(self.curr_portfolio, self._mkt_date)
        self.__is_revaluing_portfolio = False

    @staticmethod
    def __revalue_portfolio(portfolio, mkt_date):
        """ Revalues the portfolio given.

        :param portfolio:
        :param mkt_date:
        :return:
        """

        portfolio_delta = {}
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

            ao_pv01  = AirOptionMock( mkt_date
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

            portfolio_delta = Controller.__merge_deltas(portfolio_delta, ao_pv01)

        return portfolio_delta

    @staticmethod
    def __merge_deltas(delta_1 : Dict, delta_2 : Dict) -> Dict:
        """ Merge the two deltas.

        :param delta_1: delta dictionary, {'UA71': 1.,...}
        :param delta_2: delta dictionary, {'UA71': 2, 'UA72': 1.,...}
        :returns: resulting delta dictionary {'UA71': 3, 'UA72': 1.,...}
        """

        result_delta = {}

        for delta_1_flight_nb, delta_1_flight_value in delta_1.items():
            if delta_1_flight_nb in delta_2.keys():
                result_delta[delta_1_flight_nb] = delta_1_flight_value + delta_2[delta_1_flight_nb]
            else:
                result_delta[delta_1_flight_nb] = delta_1_flight_value

        for delta_2_flight_nb in set(delta_2.keys()).difference(set(delta_1.keys())):
            result_delta[delta_2_flight_nb] = delta_2[delta_2_flight_nb]

        return result_delta
