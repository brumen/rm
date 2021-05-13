# main controlling logic for the real time risk management system

import logging

from typing    import List, Callable, Tuple, Optional
from queue     import Queue
from time      import sleep
from threading import Thread


logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Controller:
    """ Controlling logic of the position updater.

    LOCAL_WORK_LIMIT: switch between local and distributed processing occurs at this point.
    """

    QUEUE_SIZE = 1000
    LOCAL_WORK_LIMIT = 700
    _NB_THREADS = 8

    def __init__( self
                , value_portfolio_fct_local : Callable
                , value_portfolio_fct_remote: Callable ):
        """ Controller class, keeps track of the system and distributes work.

        :param value_portfolio_fct_local: function computing the portfolio locally, by the controller process itself.
        :param value_portfolio_fct_remote: function computing the portfolio on spark.
        """

        self.__market_queue = Queue(maxsize=self.QUEUE_SIZE)

        # signal handlers
        self.__value_portfolio_fct_local = value_portfolio_fct_local
        self.__value_portfolio_fct_remote = value_portfolio_fct_remote

        # variables for new market and trade events.
        self.__new_market_event = False  # we get an update for the new market.
        self._new_trade_event   = False  # we get an update that a new trade arrived.

        # trade queues
        self.__trade_queue_curr_market = Queue()
        self.__trade_queue_new_market  = Queue()

        # current and new value of the portfolio on the market.
        self.__market_curr = None
        self.__market_new  = None

        self.__new_market_prev_working = False
        self.__curr_market_prev_working = False

        self.__all_trades = []

    def curr_mkt_queue_size(self):
        return self.__trade_queue_curr_market.qsize()

    def new_mkt_queue_size(self):
        return self.__trade_queue_new_market.qsize()

    @property
    def all_trades(self):
        return self.__all_trades

    def __market_working(self, curr_new : str = 'curr') -> bool:
        """ Indicator whether the current/new market is working.

        :param curr_new:
        :return:
        """

        if curr_new == 'curr':
            return not self.__trade_queue_curr_market.empty()

        return not self.__trade_queue_new_market.empty()

    def __market_finished(self, curr_new : str = 'curr') -> bool:
        """ Indicator whether the current/new market has finished.

        :param curr_new:
        :return:
        """

        if curr_new == 'curr':
            return (not self.__market_working('curr')) and self.__curr_market_prev_working

        return (not self.__market_working('new')) and self.__new_market_prev_working

    def add_position(self, new_positions : List) -> None:
        """ Adding positions to the queue: to curr_market queue only if the new market is idle, otherwise to both
            markets.

        :param new_positions: new positions to be added to the process queue.
        :returns: adds positions to the position queue and sets the new_trade_event to true
        """

        # always add positions to the current market
        logger.debug(f'Adding positions to CURR market: {len(new_positions)}')
        for new_position in new_positions:
            self.__all_trades.append(new_position)
            self.__trade_queue_curr_market.put(new_position)

        # add positions to the new market only if it's working, otherwise dont
        if self.__market_working('new'):  # add positions also to NEW queues.
            logger.debug(f'Adding positions to NEW market queue: {len(new_positions)}')
            for new_position in new_positions:
                self.__trade_queue_new_market.put(new_position)

        logger.debug(f'Positions in CURR market queue: {self.__trade_queue_curr_market.qsize()}')
        logger.debug(f'Positions in NEW  market queue: {self.__trade_queue_new_market.qsize()}')

    def add_market(self, new_market):
        """ Adds the new market event to the queue, this shouldnt be that fast.

        :param new_market: market event to be added.
        :returns: nothing, just adds the market to the market process queue and sets the __new_market_event.
        """

        logger.debug('New market event occurred.')
        self.__market_queue.put(new_market)

        if (not self.__market_working('curr')) and (not self.__market_working('new')):
            # both markets are idle (add all trades to the new market, leave the curr one alone)
            self.__market_new = None  # reset the new market
            for new_position in self.__all_trades:
                self.__trade_queue_new_market.put(new_position)

        if (not self.__market_working('curr')) and self.__market_working('new'):
            # ignore the market just being updated
            pass

        if self.__market_working('curr') and (not self.__market_working('new')):
            # add ALL positions to the new market, and start pricing it.
            self.__market_new = None
            for new_position in self.__all_trades:
                self.__trade_queue_new_market.put(new_position)

        if self.__market_working('curr') and self.__market_working('new'):
            pass

    @property
    def curr_market(self):
        """ Returns the results on the current market.

        :returns: computation results on the current market.
        """

        return self.__market_curr

    @property
    def new_market(self):
        """ Returns the results on the new market.

        :returns: computation results on the new market.
        """

        return self.__market_new

    def _get_trades_from_queue(self, curr_new_indic : str = 'curr', nb_elts : int = 1) -> List:
        """ Take the trades from the trade events queue and put them in the portfolio.

        :param curr_new_indic: indicator whether new or current queue is considered.
        :param nb_elts: number of elements to take from the queue
        :returns: list of new trades in the position queue.
        """

        trade_queue = self.__trade_queue_curr_market if curr_new_indic == 'curr' else self.__trade_queue_new_market

        new_trades = []
        curr_elt = 0
        while (not trade_queue.empty()) and (curr_elt < nb_elts):
            new_trades.append(trade_queue.get())
            curr_elt += 1

        return new_trades

    @staticmethod
    def _trade_result_agg(trade_pv_1 : Optional[float], trade_pv_2 : Optional[float]) -> float:
        """ Aggregation function for trade_1 and trade_2.

        :param trade_pv_1: pv of the first trade
        :param trade_pv_2: pv of the second trade
        :returns:
        """

        if trade_pv_1 is None:
            if trade_pv_2 is None:
                return 0.

            return trade_pv_2

        if trade_pv_2 is None:  # trade_pv_1 is not None
            return trade_pv_1

        return trade_pv_1 + trade_pv_2  # neither is None

    def __trade_processor(self, curr_new_mkt : str, sleep_delay : float = 0.1):
        """ Runs the thread processor for the current (or new) market.

        :param curr_new_mkt: choice between the current and new markets ('curr', 'new')
        :param sleep_delay: sleep delay in case of IDLE market.
        :returns: nothing, runs the thread for the current market.
        """

        trade_queue = self.__trade_queue_curr_market if curr_new_mkt == 'curr' else self.__trade_queue_new_market

        while True:

            logger.debug(f'Trades in {curr_new_mkt} queue: {trade_queue.qsize()}')
            if not trade_queue.empty():

                if curr_new_mkt == 'curr':
                    self.__curr_market_prev_working = True
                else:
                    self.__new_market_prev_working = True

                logger.debug(f'Processing trades on the {curr_new_mkt} market: {trade_queue.qsize()}.')
                # start by processing them 1 by one
                if trade_queue.qsize() < self.LOCAL_WORK_LIMIT:
                    trade_value = self.__value_portfolio_fct_local([trade_queue.get()])[0]
                    if curr_new_mkt == 'curr':
                        self.__market_curr = self._trade_result_agg( trade_value, self.__market_curr)
                    else:
                        self.__market_new = self._trade_result_agg( trade_value, self.__market_new)

                else:
                    # lots of trades, take PRESCRIBED number of trades
                    trade_values = sum(self.__value_portfolio_fct_remote(self._get_trades_from_queue(curr_new_mkt, self._NB_THREADS)))
                    if curr_new_mkt == 'curr':
                        self.__market_curr = self._trade_result_agg(trade_values, self.__market_curr)
                    else:
                        self.__market_new = self._trade_result_agg(trade_values, self.__market_new)

            else:
                if curr_new_mkt == 'new':  # only for the new market
                    if self.__new_market_prev_working:
                        self.__market_curr = self.__market_new

                # update the prev working section to set the prev working to False
                if curr_new_mkt == 'curr':
                    self.__curr_market_prev_working = False
                else:
                    self.__new_market_prev_working = False

                sleep(sleep_delay)

    def start(self, idle_delay : float = 0.1) -> Tuple[Thread, Thread]:
        """ Run the controller, start current and new market processing threads.

        :param idle_delay: delay of the IDLE state of the controller threads.
        :returns: runs the controller and activates the current and new market threads.
        """

        # new market thread, curr_mkt_thread
        curr_mkt_thread = Thread(target = lambda : self.__trade_processor('curr', sleep_delay=idle_delay) )
        curr_mkt_thread.start()

        new_mkt_thread = Thread(target = lambda : self.__trade_processor('new', sleep_delay=idle_delay) )
        new_mkt_thread.start()

        return curr_mkt_thread, new_mkt_thread
