# main controlling logic for the real time risk management system

import logging

from typing    import List, Callable, Tuple
from queue     import Queue
from time      import sleep
from threading import Thread


logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class Controller:
    """ Controlling logic of the position updater.

    LOCAL_WORK_LIMIT: switch between local and distributed processing occurs at this point.
    """

    QUEUE_SIZE = 1000
    LOCAL_WORK_LIMIT = 700

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
        self.__new_trade_event  = False  # we get an update that a new trade arrived.

        # trade queues
        self.__trade_queue_curr_market = Queue()
        self.__trade_queue_new_market  = Queue()

        # current and new value of the portfolio on the market.
        self.__market_curr = None
        self.__market_new  = None

        self.__new_market_curr_working = False  # indicator whether the new market has finished.
        self.__new_market_prev_working = False

    def add_position(self, new_positions : List) -> None:
        """ Adding positions to the queue: to curr_market queue only if the new market is idle, otherwise to both
            markets.

        :param new_positions: new positions to be added to the process queue.
        :returns: adds positions to the position queue and sets the new_trade_event to true
        """

        new_market_running = self.__new_market_curr_working

        if new_market_running:  # add positions to both NEW & CURR queues.
            for new_position in new_positions:
                self.__trade_queue_curr_market.put(new_position)
                self.__trade_queue_new_market.put(new_position)
        else:  # add position only to NEW market, new market is idle.
            for new_position in new_positions:
                self.__trade_queue_new_market.put(new_position)

        if new_market_running:
            logger.debug(f'Adding positions to CURR & NEW markets: {len(new_positions)}')
        else:
            logger.debug(f'Adding positions to CURR market: {len(new_positions)}')

        self.__new_trade_event = True

    def add_market(self, new_market):
        """ Adds the new market event to the queue, this shouldnt be that fast.

        :param new_market: market event to be added.
        :returns: nothing, just adds the market to the market process queue and sets the __new_market_event.
        """

        self.__market_queue.put(new_market)
        self.__new_market_event = True

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

    def _get_total_current_portfolio(self) -> List:
        """ Get the current portfolio to be used with market event.

        :returns: The current portfolio to be priced.
        """

        # return the current portfolio
        raise NotImplementedError('You have to implement _get_total_current_portfolio.')

    def _get_trades_from_queue(self, curr_new_indic : str = 'curr') -> List:
        """ Take the trades from the trade events queue and put them in the portfolio.

        :param curr_new_indic: indicator whether new or current queue is considered.
        :returns: list of new trades in the position queue.
        """

        trade_queue = self.__trade_queue_curr_market if curr_new_indic == 'curr' else self.__trade_queue_new_market

        new_trades = []
        while not trade_queue.empty():
            new_trades.append(trade_queue.get())

        return new_trades

    @staticmethod
    def _combine_results(new_results, old_results):
        """ Combine the new and old results.

        :param new_results: new results to be added to the results.
        :param old_results: old saved results.
        :returns: joins the two results.
        """

        if not old_results:  # old results are None at the start
            return new_results

        return new_results + old_results

    def __trade_processor_curr(self, sleep_delay : float = 0.1):
        """ Runs the thread processor for the current market.

        :param sleep_delay: sleep delay in case of IDLE market.
        :returns: nothing, runs the thread for the current market.
        """

        while True:
            if not self.__trade_queue_curr_market.empty():
                trades_to_process = self._get_trades_from_queue('curr')
                self.__market_curr = self._combine_results( self.__worker_fct(len(trades_to_process))(trades_to_process)
                                                          , self.__market_curr)

            else:
                sleep(sleep_delay)

    def __worker_fct(self, nb_trades_to_process : int ) -> Callable:
        """ Worker function, depending on how many trades are still to process.

        :param nb_trades_to_process: number of trades to process
        :returns: a function that processes the trades.
        """

        return self.__value_portfolio_fct_local if nb_trades_to_process < self.LOCAL_WORK_LIMIT else self.__value_portfolio_fct_remote

    def __trade_processor_new(self, sleep_delay : float = 0.1):
        """ Runs the thread processor for the new market.

        :returns None: schedules the new market computations, works on queues, etc.
        """

        while True:
            self.__new_market_prev_working = self.__new_market_curr_working  # prev <- curr

            # new market processing is not yet completed, still trades to process
            if not self.__trade_queue_new_market.empty():  # work to do.
                self.__new_market_curr_working = True
                self.__new_market_event = False  # ignoring all the further market events.

                trades_to_process = self._get_trades_from_queue('new')  # get the whole portfolio
                self.__market_new = self._combine_results( self.__worker_fct(len(trades_to_process))(trades_to_process)
                                                         , self.__market_new)

            else:  # queue is empty. either we just finished working or we didnt work at all

                if not self.__new_market_event:  # no new market event, nothing to do.
                    self.__new_market_curr_working = False
                    sleep(sleep_delay)

                else:  # new market event, start working
                    self.add_position(self._get_total_current_portfolio())
                    self.__new_market_event = False
                    self.__new_market_curr_working = True

            # check if there is a need to switch the markets, and switch it if yes.
            if self.__new_market_prev_working and (not self.__new_market_curr_working):  # we finished work, switch markets
                self.__market_curr = self.__market_new  # IMPORTANT: switch markets
                self.__market_new = None  # reset of the new market.

    def start(self, idle_delay : float = 0.1) -> Tuple[Thread, Thread]:
        """ Run the controller, start current and new market processing threads.

        :param idle_delay: delay of the IDLE state of the controller threads.
        :returns: runs the controller and activates the current and new market threads.
        """

        # new market thread, curr_mkt_thread
        curr_mkt_thread = Thread(target = lambda : self.__trade_processor_curr(sleep_delay=idle_delay) )
        curr_mkt_thread.start()

        new_mkt_thread = Thread(target = lambda : self.__trade_processor_new(sleep_delay=idle_delay) )
        new_mkt_thread.start()

        return curr_mkt_thread, new_mkt_thread
