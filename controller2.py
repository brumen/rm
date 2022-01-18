""" Main/Basic controlling logic for the real time risk management system.
"""

import logging

from typing    import List, Tuple, Optional, Generator, Any, Union
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
    LOCAL_WORK_LIMIT = 70000000
    _NB_THREADS = 8

    def __init__( self ):
        """ Controller class, keeps track of the system and distributes work.
        """

        self.__market_queue = Queue(maxsize=self.QUEUE_SIZE)

        # variables for new market and trade events.
        self._new_market_event = False  # we get an update for the new market.
        self._new_trade_event  = False  # we get an update that a new trade arrived.

        # trade queues
        self._trade_queue_curr_market = Queue()
        self._trade_queue_new_market  = Queue()

        # market object
        self._market_obj = None

        # current and new value of the portfolio on the market.
        self.__market_curr = None
        self.__market_new  = None

        self.__new_market_prev_working = False
        self.__curr_market_prev_working = False

        self.__all_trades = []

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

    def curr_mkt_queue_size(self):
        return self._trade_queue_curr_market.qsize()

    def new_mkt_queue_size(self):
        return self._trade_queue_new_market.qsize()

    @property
    def all_trades(self):
        return self.__all_trades

    def __market_working(self, curr_new : str = 'curr') -> bool:
        """ Indicator whether the current/new market is working.

        :param curr_new: indicator whether this is current or new market.
        :returns: true/false depending on whether the desired market processor is working.
        """

        if curr_new == 'curr':
            return not self._trade_queue_curr_market.empty()

        return not self._trade_queue_new_market.empty()

    def add_market(self, new_market):
        """ Adds the new market event to the queue, this shouldnt be that fast.

        :param new_market: market event to be added.
        :returns: nothing, just adds the market to the market process queue and sets the __new_market_event.
        """

        logger.debug('New market event occurred.')
        self.__market_queue.put(new_market)

        if not self.__market_working('new'):
            # both markets are idle (add all trades to the new market, leave the curr one alone)
            self.__market_new = None  # reset the new market
            for new_position in self.__prune_offsetting_trades(self.__all_trades):
                self._trade_queue_new_market.put(new_position)

        # BOTTOM TWO ARE NOT NEEDED, I LEFT THEM IN TO ILLUSTRATE THAT THEY ARE NOT NEEDED.
        # if (not self.__market_working('curr')) and self.__market_working('new'):
        #     # ignore the market just being updated
        #     pass
        #
        # if self.__market_working('curr') and self.__market_working('new'):
        #     pass

    @staticmethod
    def __prune_offsetting_trades(trades : List[Tuple[int, str]]) -> List[Tuple[int, str]]:
        """ Prunes the offsetting trades

        :param trades: list of trades to be pruned
        :returns: similar list, but without off-setting trades.
        """

        prunned_positions = []

        for pos_id, pos_direct in trades:
            if pos_direct == 'c':
                prunned_positions.append((pos_id, pos_direct))

            elif pos_direct == 'd':
                equiv_create_pos = (pos_id, 'c')
                if equiv_create_pos in prunned_positions:
                    equiv_pos_idx = prunned_positions.index(equiv_create_pos)
                    prunned_positions.pop(equiv_pos_idx)
            else:
                prunned_positions.append((pos_id, pos_direct))

        return prunned_positions

    def _get_trades_from_queue(self, trade_queue : Queue, nb_elts : int = 1) -> Generator[Any, None, None]:
        """ Take the trades from the trade events queue and put them in the portfolio.

        :param trade_queue: the queue from which the elts are taken.
        :param nb_elts: number of elements to take from the queue
        :returns: list of new trades in the position queue.
        """

        curr_elt = 0
        while (not trade_queue.empty()) and (curr_elt < nb_elts):
            yield trade_queue.get()
            curr_elt += 1

    @staticmethod
    def _trade_result_agg_single(trade_pv_1 : Optional[float], trade_pv_2 : Optional[float]) -> float:
        """ Aggregation function for trade_1 and trade_2.

        :param trade_pv_1: pv of the first trade
        :param trade_pv_2: pv of the second trade
        :returns: aggregated value of the two trade positions.
        """

        if trade_pv_1 is None:
            if trade_pv_2 is None:
                return 0.

            return trade_pv_2

        if trade_pv_2 is None:  # trade_pv_1 is not None
            return trade_pv_1

        return trade_pv_1 + trade_pv_2  # neither is None

    def _value_portfolio_local(self, trades : Union[List, Generator]) -> Union[List, Generator]:
        """ Values the portfolio of trades, has to be implemented in the subclass.

        :param trades: list of trades
        :returns: list of results
        """

        raise NotImplementedError(f'_value_portfolio_local has to be implemented in the class.')

    def _value_portfolio_remote(self, trades : Union[List, Generator]) -> Union[List, Generator]:
        """ Values the portfolio of trades, has to be implemented in the subclass.

        :param trades: list of trades
        :returns: list of results
        """

        raise NotImplementedError(f'_value_portfolio_remote has to be implemented in the class.')

    def __trade_processor(self, curr_new_mkt : str, sleep_delay : float = 0.1):
        """ Runs the thread processor for the current (or new) market.

        :param curr_new_mkt: choice between the current and new markets ('curr', 'new')
        :param sleep_delay: sleep delay in case of IDLE market.
        :returns: nothing, runs the thread for the current market.
        """

        trade_queue = self._trade_queue_curr_market if curr_new_mkt == 'curr' else self._trade_queue_new_market

        while True:

            logger.debug(f'Trades in {curr_new_mkt} queue: {trade_queue.qsize()}')
            if not trade_queue.empty():

                if curr_new_mkt == 'curr':
                    self.__curr_market_prev_working = True
                else:
                    self.__new_market_prev_working = True

                logger.debug(f'Processing trades on the {curr_new_mkt} market: {trade_queue.qsize()}.')
                # start by processing them 1 by one
                queue_size = trade_queue.qsize()

                if queue_size < self.LOCAL_WORK_LIMIT:
                    trade_values = self._value_portfolio_local(self._get_trades_from_queue(trade_queue, nb_elts=queue_size))
                else:
                    trade_values = self._value_portfolio_remote(self._get_trades_from_queue(trade_queue, nb_elts = queue_size // self._NB_THREADS ))

                for curr_trade_val in trade_values:
                    if curr_new_mkt == 'curr':
                        self.__market_curr = self._trade_result_agg_single(self.__market_curr, curr_trade_val)
                    else:
                        self.__market_new = self._trade_result_agg_single(self.__market_new, curr_trade_val)

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

    def _event_reaction(self, sleep_delay : float = 0.1):
        """ Reacts to certain things.

        :param sleep_delay: sleep delay in case of IDLE market.
        :returns: nothing, runs the thread for the current market.
        """

        while True:
            sleep(sleep_delay)

    def start(self, idle_delay : float = 0.1) -> List[Thread]:
        """ Run the controller, start current and new market processing threads.

        :param idle_delay: delay of the IDLE state of the controller threads.
        :returns: runs the controller and activates the current and new market threads.
        """

        # new market thread, curr_mkt_thread
        curr_mkt_thread = Thread(target = lambda : self.__trade_processor('curr', sleep_delay=idle_delay) )
        curr_mkt_thread.start()

        new_mkt_thread = Thread(target = lambda : self.__trade_processor('new', sleep_delay=idle_delay) )
        new_mkt_thread.start()

        reaction_thread = Thread(target = lambda : self._event_reaction(sleep_delay=idle_delay))
        reaction_thread.start()

        return [curr_mkt_thread, new_mkt_thread, reaction_thread]
