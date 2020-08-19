# concrete implementation of the controller.

import datetime

from typing    import List, Tuple
from pyspark   import SparkContext
from threading import Thread
from kafka     import KafkaConsumer

from rm.controller2       import Controller
from rm.positions_updater import PositionUpdaterKafka
from rm.market_ticker     import MarketUpdater

from ao.air_option  import AirOptionMock


class ControllerJoke(Controller):
    """ Specification for the workers.
    """

    def __init__(self):
        """
        """

        super().__init__(self._value_portfolio_fct)

    def _get_total_current_portfolio(self) -> List:
        return ['TRADE1'] * 100

    def _value_portfolio_fct(self, new_trades : List):
        """ Defines the portfolio_function from trades -> results.

        :returns: results of computation of the portfolio_function of these new trades.
        """

        print("HHH {0}".format(len(new_trades)))
        return len(new_trades)


class ControllerAO(Controller):
    """ Controller for AirOptions.
    """

    def __init__(self
                , trade_producer  : PositionUpdaterKafka
                , market_producer : MarketUpdater
                , topic_to_read_from : str = 'quickstarter-events' ):
        """ Initiates the Controller for computing the AirOptions portfolio.
        """

        self.__trade_producer  = trade_producer
        self.__market_producer = market_producer
        self.__listener = KafkaConsumer(topic_to_read_from)

        self.sc = SparkContext()  # TODO: THIS SHOULD BE PROPERLY DEFINED
        super().__init__(self._value_portfolio_fct)

    def _value_trade(self, trade):
        """ Returns the value of the Mock Air Option trade.

        :param trade: trade identifier.
        :returns:
        """

        air_option = AirOptionMock( datetime.date(2019, 7, 2)
                                  , origin = 'SFO'
                                  , dest = 'EWR'
                                  , K = 1600.)

        return air_option.PV()

    def _value_portfolio_fct(self, new_trades):
        """ Defines the portfolio_function from trades -> results.

        :return:
        """

        return self.sc.range(new_trades).filter(self._value_trade)  # TODO: THIS IS BULLSHIT, BUT AT LEAST SOMETHING

    def _read_from_topic(self):

        for msg in self.__listener:
            if msg == 'position':
                self.add_position(msg)
            elif msg == 'market':
                self.add_market(msg)

    def start(self, idle_delay : float = 0.1) -> Tuple[Thread, Thread, Thread, Thread]:
        """ Run the controller.

        :param idle_delay: delay of the IDLE state of the controller.
        :returns: runs all the threads of the controller and returns the thread handles.
        """

        controller_thread = super().start(idle_delay)
        market_producer_thread = self.__market_producer.start(idle_delay=idle_delay)
        position_producer_thread = self.__trade_producer.start(idle_delay=idle_delay)
        # listener thread.
        listener_thread = Thread(target=self._read_from_topic, daemon=True)
        listener_thread.start()

        return controller_thread, position_producer_thread, market_producer_thread, listener_thread


# from rm.market_ticker import MarketUpdater
# from rm.positions_updater import PositionUpdaterKafka


# class ControllerWithInputs(Controller):
#
#     def __init__(self, server : str, port : int = '9092'):
#         self.position_updater = PositionUpdaterKafka(server, port)
#         self.market_updater   = MarketUpdater(server, port)
#
#         super().__init__(VALUE_PORTFOLIO_FCT)
#         # TODO:
