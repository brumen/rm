# main controlling logic for the risk management

import logging

from typing    import List, Tuple, Callable
from queue     import Queue
from enum      import Enum
from time      import sleep
from threading import Thread


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class ControllerState(Enum):
    """ State of the controller.

    IDLE = No work.
    TRADE_REVAL = revaluing some trades
    MARKET_REVAL = revaluing market
    """

    IDLE         = 1
    TRADE_REVAL  = 2
    MARKET_REVAL = 3


class Controller:
    """ Controlling logic of the position updater.
    """

    QUEUE_SIZE = 1000

    def __init__(self, value_portfolio_fct : Callable ):
        """ Controller class, keeps track of the system and distributes work.

        :param value_portfolio_fct: function computing the given portfolio.
        """

        self.__position_queue     = Queue(maxsize=self.QUEUE_SIZE)
        self.__market_queue       = Queue(maxsize=self.QUEUE_SIZE)

        # signal handlers
        self._portfolio               = []  # initially empty portfolio
        self.__value_portfolio_fct    = value_portfolio_fct  # this function has to be non-blocking

        # variables for new market and trade events.
        self.__new_market_event = False  # we get an update for the new market.
        self.__new_trade_event  = False  # we get an update that a new trade arrived.

        # trade queues
        self.__trade_queue_curr_market = Queue()
        self.__trade_queue_new_market  = Queue()

        # processing threads
        self.__processing_curr_queue_thread = None
        self.__processing_new_queue_thread  = None

        # current and new value of the portfolio on the market.
        self.__market_curr = None
        self.__market_new  = None

    def add_position(self, new_positions : List) -> None:
        """ Adding positions to the queue.

        :param new_positions: new positions to be added to the process queue.
        :returns: adds positions to the position queue and sets the new_trade_event to true
        """

        for new_position in new_positions:
            self.__position_queue.put(new_position)
        self.__new_trade_event = True

    def add_market(self, new_market):
        """ Adds the new market event to the queue, this shouldnt be that fast.

        :param new_market: market event to be added.
        :returns: nothing, just adds the market to the market process queue and sets the __new_market_event.
        """

        self.__market_queue.put(new_market)
        self.__new_market_event = True

    def _controller_state(self):
        """ Returns the state of the controller.

        :returns: current state of the controller.
        """

        if self.__processing_queue('new'):
            return ControllerState.MARKET_REVAL  # market revaluation state.

        if self.__processing_queue('curr'):
            return ControllerState.TRADE_REVAL  # trade revaluation

        return ControllerState.IDLE

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

        :return:
        """

        # return the current portfolio
        raise NotImplementedError('You have to implement _get_total_current_portfolio.')

    def _get_new_trades(self) -> List:
        """ Take the trades from the trade events queue and put them in the portfolio.

        :returns: list of new trades in the position queue.
        """

        new_trades = []
        while not self.__position_queue.empty():
            new_trades.append(self.__position_queue.get())

        return new_trades

    def _revalue_new_trades(self, new_trades : List):
        """ Do something with new trades. This function is a GENERATOR.

        :param new_trades: new trades to be processed.
        :returns: trades
        """

        # TODO: CHECK - THIS MIGHT BE WRONG HERE
        new_trade_result = self.__value_portfolio_fct(new_trades)
        self.__new_trade_event = False  # new trade events are processed

        return new_trade_result

    def combine_results(self, new_results, old_results):
        raise NotImplementedError('You should overwrite this function.')

    def __processing_queue(self, curr_new_indic : str = 'curr') -> bool:
        """ Answers the question if the current/new queue is being processed.

        :param curr_new_indic: indicator which thread to question, possibilities: 'curr', 'new'
        :returns: True/False whether the thread is working or not.
        """

        chosen_thread = self.__processing_curr_queue_thread if curr_new_indic == 'curr' else self.__processing_new_queue_thread

        return False if not chosen_thread else chosen_thread.isAlive()

    def _state_machine(self):
        """ Manipulation of the state machine.

        :return:
        """

        if self._controller_state() == ControllerState.IDLE:
            if self.__new_trade_event:  # we have a new trade event
                if not self.__processing_queue('curr'):
                    self.__processing_curr_queue_thread = Thread(target = lambda : self.__process_curr_new_market_queue('curr'), daemon=True)
                    self.__processing_curr_queue_thread.start()
                    self.__new_trade_event = False
                else:
                    # we know that the new trade event is already in the queue, so we can reset it.
                    self.__new_trade_event = False

            if self.__new_market_event:
                if not self.__processing_queue('new'):
                    self.__processing_new_queue_thread = Thread(target = lambda : self.__process_curr_new_market_queue('new'), daemon=True)
                    self.__processing_new_queue_thread.start()
                    self.__new_market_event = False
                else:
                    self.__new_market_event = False  # TODO: THIS SHOULDNT BE REACHED - PROGRAM BETTER

        if self._controller_state() == ControllerState.TRADE_REVAL:  # revaluing some trades, but not the whole market.

            if self.__new_trade_event:  # trade event is added to the queue, just leave it running
                self.__new_trade_event = False  # already processing, let it finish, event already added to the queue

            elif self.__new_market_event:
                # new market event
                if not self.__processing_queue('new'):
                    self.__processing_new_queue_thread = Thread(target=lambda : self.__process_curr_new_market_queue('new'), daemon=True)
                    self.__processing_new_queue_thread.start()
                    self.__new_market_event = False
                else:
                    self.__new_market_event = False

        if self._controller_state() == ControllerState.MARKET_REVAL:  # already revaluing whole market

            # market event still processing
            if self.__new_market_event:  # we got a new market event in between processing
                self.__new_market_event = False

            elif self.__new_trade_event:
                new_trades_to_price = self._get_new_trades()
                self.__trade_queue_curr_market.put(new_trades_to_price)
                self.__trade_queue_new_market.put(new_trades_to_price)
                self.__new_trade_event = False

    def __process_curr_new_market_queue(self, curr_new_indic : str = 'curr') -> None:
        """ Processes current or new market queue, adds to current or new market result.

        :param curr_new_indic: 'curr' if working on current queue/current market, otherwise 'new' market, or 'total' for total portfolio
                               'new'
                               'total'
        :returns: updates the market_curr, market_new
        """

        if curr_new_indic == 'curr':
            trade_queue = self.__trade_queue_curr_market
            market      = self.__market_curr

        else:  #  curr_new_indic == 'new':
            trade_queue = self.__trade_queue_new_market
            market      = self.__market_new

        # depending on the trade queue
        if curr_new_indic == 'new':  # switch the market
            if trade_queue.empty():
                self.__market_curr = self.__market_new  # TODO: CHECK IF THIS IS TRUE

        else:
            new_trade = trade_queue.get()  # scheduling mechanism # TODO: THIS SHOULD BE BETTER.
            market = self.combine_results(market, self._revalue_new_trades(new_trade))  # updating the market

        trade_queue.task_done()  # TODO: CHECK HERE

    def __run_function(self, idle_delay = 0.1):
        """ Run the state machine

        :param idle_delay:
        :return:
        """

        while True:
            self._state_machine()
            if self._controller_state() == ControllerState.IDLE:
                # wait some time
                sleep(idle_delay)

    def run(self, idle_delay = 0.1) -> Thread:
        run_thread = Thread(target = self.__run_function, kwargs={'idle_delay': 0.1} )
        run_thread.start()

        return run_thread
