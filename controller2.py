# main controlling logic for the risk management

import logging

from typing    import List, Callable
from queue     import Queue
from enum      import Enum
from time      import sleep
from threading import Thread


logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


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

        self.__position_queue = Queue(maxsize=self.QUEUE_SIZE)
        self.__market_queue   = Queue(maxsize=self.QUEUE_SIZE)

        # signal handlers
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
            self.__trade_queue_curr_market.put(new_position)
            self.__trade_queue_new_market.put(new_position)  # TODO: DOES THIS MAKE SENSE THIS IS WRONG WRONG
        logger.info('')
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

        :returns: The current portfolio to be priced.
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

    def _combine_results(self, new_results, old_results):
        """ Combine the new and old results.

        :param new_results: new results.
        :param old_results: old results.
        :returns: joins the two results.
        """

        if not old_results:  # old results are None at the start
            print("SSS {0}".format(new_results))
            return new_results

        print('SSS {0}'.format(new_results + old_results))
        return new_results + old_results

    def __processing_queue(self, curr_new_indic : str = 'curr') -> bool:
        """ Answers the question if the current/new queue is being processed.

        :param curr_new_indic: indicator which thread to question, possibilities: 'curr', 'new'
        :returns: True/False whether the thread is working or not.
        """

        chosen_thread = self.__processing_curr_queue_thread if curr_new_indic == 'curr' else self.__processing_new_queue_thread

        return False if not chosen_thread else chosen_thread.isAlive()

    def __start_thread(self, curr_new_indic):
        """ Starting the queue depending on the 'new', 'curr' market.

        :param str curr_new_indic: current, new indicator
        :returns: none, just starts the relevant thread.
        """

        queue_thread = Thread(target=lambda: self.__process_curr_new_market_queue(curr_new_indic), daemon=True)
        queue_thread.start()

    def _state_machine(self):
        """ Function of the state machine.

        :returns: nothing, runs the state machine.
        """

        controller_state = self._controller_state()

        logger.debug('Controller in state {0}'.format(controller_state))

        if controller_state == ControllerState.IDLE:

            if self.__new_trade_event:  # we have a new trade event
                logger.debug('New trade event detected.')

                if not self.__processing_queue('curr'):
                    logger.debug('Starting current trade processing thread.')
                    self.__start_thread('curr')

                self.__new_trade_event = False

            if self.__new_market_event:
                logger.debug('New market event detected.')

                if not self.__processing_queue('new'):
                    logger.debug('Starting the new market processing threads.')
                    self.__start_thread('new')

                self.__new_market_event = False

        if controller_state == ControllerState.TRADE_REVAL:  # revaluing some trades, but not the whole market.

            if self.__new_trade_event:  # trade event is added to the queue, just leave it running
                self.__new_trade_event = False  # already processing, let it finish, event already added to the queue

            elif self.__new_market_event:
                if not self.__processing_queue('new'):
                    logger.debug('Starting new market processing thread.')
                    self.__start_thread('new')
                self.__new_market_event = False

        if controller_state == ControllerState.MARKET_REVAL:  # already revaluing whole market

            # market event still processing
            if self.__new_market_event:  # we got a new market event in between processing
                self.__new_market_event = False

            elif self.__new_trade_event:  # market revaluation is working.
                logger.debug('Adding new trades to the current/new processing queue.')

                # new_trades_to_price = self._get_new_trades()
                # self.__trade_queue_curr_market.put(new_trades_to_price)
                # self.__trade_queue_new_market.put(new_trades_to_price)
                self.__new_trade_event = False

    def __process_curr_new_market_queue(self, curr_new_indic : str = 'curr') -> None:
        """ Processes current or new market queue, adds to current or new market result.

        :param curr_new_indic: 'curr' if working on current queue/current market, otherwise 'new' market, or 'total' for total portfolio
                               'new'
                               'total'
        :returns: updates the market_curr, market_new
        """

        logger.debug('Thread processor; Market selection: {0}'.format(curr_new_indic))

        trade_queue = self.__trade_queue_curr_market if curr_new_indic == 'curr' else self.__trade_queue_new_market

        if curr_new_indic == 'new':  # new market
            print('here4')
            if trade_queue.empty():  # switch the market, otherwise continue the calculations
                print('here5')
                self.__market_curr = self.__market_new
            else:
                print('here2')
                self.__market_new = self._combine_results(self.__value_portfolio_fct(trade_queue.get()), self.__market_new )  # updating the market
                trade_queue.task_done()

        else:  # 'curr' market
            print('here6', '{0}'.format(trade_queue.qsize()))
            if not trade_queue.empty():
                print('here3')
                self.__market_curr = self._combine_results(self.__value_portfolio_fct(trade_queue.get()), self.__market_curr)  # updating the market
                trade_queue.task_done()

    def __run_function(self, idle_delay : float = 0.1):
        """ Run the state machine function.

        :param idle_delay: delay for the IDLE state of the controller.
        :returns: nothing, runs the controller logic.
        """

        while True:
            self._state_machine()
            if self._controller_state() == ControllerState.IDLE:  # idle state is slowed down.
                sleep(idle_delay)

    def start(self, idle_delay : float = 0.1) -> Thread:
        """ Run the controller.

        :param idle_delay: delay of the IDLE state of the controller.
        :returns: run the controller.
        """

        run_thread = Thread(target = self.__run_function, kwargs={'idle_delay': idle_delay} )
        run_thread.start()

        return run_thread
