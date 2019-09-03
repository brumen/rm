#
# a worker class that receives the portfolio and computes delta from it, returns the delta back to the controller.
#

import datetime
import logging
import time
import json
import threading

from delta_dict             import DeltaDict
from socket_msg             import NanoSocketMixin

from ao.air_option          import AirOptionMock

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class PortfolioAirWorker:

    def __init__(self
                 , socket  : NanoSocketMixin.Socket  # PAIR recv, send_socket
                 , mkt_date  = None
                 , worker_name ='Gorazd'
                 , sleep_time  = 0.0001
                 , ):
        """ Worker process class.

        :param socket: PAIR nanomsg worker recv_socket
        :param mkt_date: market date TODO: TO BE REMOVED LATER!!!
        :param worker_name: host name of the worker, used for identification.
        :param sleep_time: sleep time between iteration on the working thread.
        """

        self.socket      = socket
        self.mkt_date      = mkt_date
        self.__worker_name = worker_name
        self.__sleep_time = sleep_time

        # signal handlers
        self.__is_revaluing_portfolio = False

    def is_working(self):
        return self.__is_revaluing_portfolio

    @staticmethod
    def _decode_msg(msg):
        """ How to decode the message we received from controller.

        :param msg: message received.
        :returns: python object representation of the message.
        """

        return json.loads(msg.decode('utf-8'))

    @staticmethod
    def _encode_msg(obj):
        """ Encoding the object, using json.

        :param obj: object to encode
        :return:
        """

        return json.dumps(obj)  # obj is the delta object

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
            revalued_portfolio = self.__class__.revalue_portfolio(self.__class__._decode_msg(msg_received), self.mkt_date)  # DeltaDict
            self.socket.send(self.__class__._encode_msg(revalued_portfolio))
            self.__is_revaluing_portfolio = False
            time.sleep(self.__sleep_time)

    @staticmethod
    def revalue_portfolio(portfolio, mkt_date : datetime.date) -> DeltaDict:
        """ Revalues the portfolio given.

        :param portfolio: trade portfolio to use.
        :param mkt_date: market date
        :return:
        """

        logger.debug('Computing portfolio {0}'.format(str(portfolio)))
        portfolio_delta = DeltaDict({})

        for _, orig\
             , dest\
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
