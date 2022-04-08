""" Main/Basic controlling logic for the real time risk management system.
"""

import logging

from typing    import List, Tuple, Optional, Generator, Any, Union
from queue     import Queue
from time      import sleep
from threading import Thread

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Controller:
    """ Controlling logic of the position updater.

    LOCAL_WORK_LIMIT: switch between local and distributed processing occurs at this point.
    """

    QUEUE_SIZE = 1000
    LOCAL_WORK_LIMIT = 70000000
    _NB_THREADS = 8

    def __init__( self, local_only : bool = False ):
        """ Controller class, keeps track of the system and distributes work.

        :param local_only: only use local service to run the computations.
        """

        self._local_only : bool = local_only

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

        # market snaps
        initial_market = self._snap_market()
        self._market_snap_curr = initial_market
        self._market_snap_new  = initial_market  # TODO: CHECK IF THIS IS OK??

    def _snap_market(self):
        raise NotImplementedError(f'Implement this method')

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

    def new_mkt_event(self) -> None:
        """ Adds the new market event to the queue, this shouldnt be that fast.

        :returns: nothing, just adds the market to the market process queue and sets the __new_market_event.
        """

        logger.debug('New market event occurred.')

        if self._trade_queue_new_market.empty():
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
            self._trade_queue_curr_market.put(new_position)

        # add positions to the new market only if it's working, otherwise dont
        if not self._trade_queue_new_market.empty():  # new market is working, add positions also to NEW queues.
            logger.debug(f'Adding positions to NEW market queue: {len(new_positions)}')
            for new_position in new_positions:
                self._trade_queue_new_market.put(new_position)

        logger.debug(f'CURRENT market positions: {self._trade_queue_curr_market.qsize()}')
        logger.debug(f'NEW     market positions: {self._trade_queue_new_market.qsize()}')

    @staticmethod
    def __prune_offsetting_trades(trades : List[Tuple[int, str]]) -> List[Tuple[int, str]]:
        """ Prunes the offsetting trades

        :param trades: list of trades to be pruned
        :returns: similar list, but without off-setting trades.
        """

        pruned_positions = []

        for pos_id, pos_direct in trades:
            if pos_direct == 'c':
                pruned_positions.append((pos_id, pos_direct))

            elif pos_direct == 'd':
                equiv_create_pos = (pos_id, 'c')
                if equiv_create_pos in pruned_positions:
                    equiv_pos_idx = pruned_positions.index(equiv_create_pos)
                    pruned_positions.pop(equiv_pos_idx)
            else:
                pruned_positions.append((pos_id, pos_direct))

        return pruned_positions

    @staticmethod
    def _get_trades_from_queue(trade_queue : Queue, nb_elts : int = 1) -> Generator[Any, None, None]:
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

    def _value_portfolio_local(self, trades : Union[List, Generator], params) -> Union[List, Generator]:
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

    def _replace_curr_with_new_mkt(self) -> bool:
        """ Indicator whether to switch: curr_market <- new market

        :returns: True if the switch should be done, otherwise False
        """

        if self._trade_queue_new_market.empty() and self.__new_market_prev_working:
            logger.debug('Switching curr_market <- new_market')
            return True

        return False

    def _trade_processor_curr(self, sleep_delay: float = 0.1):
        """ Runs the thread processor for the current market.

        :param sleep_delay: sleep delay in case of IDLE market.
        :returns: nothing, runs the thread for the current market.
        """

        trade_queue = self._trade_queue_curr_market

        while True:
            queue_size = trade_queue.qsize()
            if not trade_queue.empty():
                logger.info(f'CURRENT queue: working on {queue_size} trades')
                self.__curr_market_prev_working = True
                trade_values = self._evaluate_trades(queue_size, trade_queue, self._market_snap_curr)
                for curr_trade_val in trade_values:
                    self.__market_curr = self._trade_result_agg_single(self.__market_curr, curr_trade_val)

            else:
                # update the prev working section to set the prev working to False
                logger.info(f'CURRENT queue: nothing to do, sleeping {sleep_delay} secs.')
                self.__curr_market_prev_working = False
                sleep(sleep_delay)

            print(self.__market_curr)

    def _evaluate_trades(self, queue_size : int, trade_queue : Queue, market_snap ) -> List[Any]:
        """ Computes the trade metric for the queue_size of trades in trade_queue.

        :param queue_size: take this number of trades from trade_queue.
        :param trade_queue: queue from which the trades are taken.
        :param market_snap: snap of the market on which we want to price the trades.
        :returns List[Any]: List of trade values, doesnt have to be PV, could be some other metric,
             like delta.
        """

        # nb_elts_to_take = 10
        # trade_values = self._value_portfolio_local(self._get_trades_from_queue(trade_queue, nb_elts=nb_elts_to_take), self._market_snap_new)

        if queue_size < self.LOCAL_WORK_LIMIT or self._local_only:  # compute locally
            return self._value_portfolio_local(self._get_trades_from_queue(trade_queue, nb_elts=queue_size)
                                              , market_snap
                                              , )

        # compute this remotely.
        return self._value_portfolio_remote( self._get_trades_from_queue(trade_queue, nb_elts=queue_size // self._NB_THREADS))  # TODO: FIX THIS HERE

    def _trade_processor_new(self, sleep_delay : float = 0.1):
        """ Runs the thread processor for the NEW market.

        :param sleep_delay: sleep delay in case of IDLE market.
        :returns: nothing, runs the thread for the current market.
        """

        trade_queue = self._trade_queue_new_market

        while True:
            queue_size = trade_queue.qsize()
            if not trade_queue.empty():  # queue not empty, continue working
                logger.debug(f'NEW market: Computing {queue_size} trades.')
                self.__new_market_prev_working = True
                trade_values = self._evaluate_trades(queue_size, trade_queue, self._market_snap_new)
                for curr_trade_val in trade_values:
                    self.__market_new = self._trade_result_agg_single(self.__market_new, curr_trade_val)

            else:  # queue is empty,
                if self._replace_curr_with_new_mkt():  # this is equivalent to the statement above
                    logger.info(f'NEW market: Switching: curr market <- new market .')
                    self.__market_curr = self.__market_new

                    # new snaps of the market
                    self._market_snap_curr = self._market_snap_new
                    self._market_snap_new  = self._snap_market()  # new market

                    # update the prev working section to set the prev working to False
                    self.__new_market_prev_working = False
                else:
                    logger.info(f'NEW market: nothing to do, waiting {sleep_delay} secs.')
                    self.__new_market_prev_working = False
                    sleep(sleep_delay)

    def start(self, idle_delay : float = 1.) -> List[Thread]:
        """ Run the controller, start current and new market processing threads.

        :param idle_delay: delay of the IDLE state of the controller threads.
        :returns: runs the controller and activates the current and new market threads.
        """

        # new market thread, curr_mkt_thread
        curr_mkt_thread = Thread(target = lambda : self._trade_processor_curr(sleep_delay=idle_delay) )
        curr_mkt_thread.start()

        new_mkt_thread = Thread(target = lambda : self._trade_processor_new(sleep_delay=idle_delay) )
        new_mkt_thread.start()

        return [curr_mkt_thread, new_mkt_thread]


# sample start of the controller
# controller = ControllerAO()
# controller.start()
