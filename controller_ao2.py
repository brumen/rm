# concrete implementation of the controller.

import datetime
import logging

from typing    import List, Tuple
from pyspark   import SparkContext
from threading import Thread
from kafka     import KafkaConsumer

from rm.controller2       import Controller
from rm.positions_updater import PositionUpdater
from rm.market_ticker     import MarketUpdater

from ao.air_option  import AirOptionMock


logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


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

        return len(new_trades)


class ControllerAO(Controller):
    """ Controller for AirOptions.
    """

    def __init__(self
                , trade_producer  : PositionUpdater
                , market_producer : MarketUpdater
                , topic_to_read_from : str = 'quickstart-events' ):
        """ Initiates the Controller for computing the AirOptions portfolio.

        :param trade_producer: trade producer, publishes to Kafka.
        :param market_producer: produces market events, also publishes to Kafka.
        :param topic_to_read_from: topic on Kafka to read from.
        """

        self.__trade_producer  = trade_producer
        self.__market_producer = market_producer
        self.__listener = KafkaConsumer(topic_to_read_from)

        super().__init__(self._value_portfolio_fct)

        # cached values
        self.__sc = None  # spark context

    @property
    def sc(self) -> SparkContext:
        """ Spark context definition.

        :return:
        """

        if self.__sc:
            return self.__sc

        self.__sc = SparkContext()
        return self.__sc

    def _value_trade(self, trade):
        """ Returns the value of the Mock Air Option trade.

        :param trade: trade identifier.
        :returns:
        """

        air_option = AirOptionMock( datetime.date(2019, 7, 2)
                                  , origin = 'SFO'
                                  , dest = 'EWR'
                                  , K = 1600.).PV()

        logger.debug('Value trade: {0}'.format(air_option))
        return air_option

    def _value_portfolio_fct(self, new_trades):
        """ Defines the portfolio_function from trades -> results.

        :return:
        """

        # TODO: this is useless, but it produces something.
        return self._value_trade(1.)
        #return self.sc.range(1)\
        #              .map(self._value_trade)\
        #              .aggregate(0., lambda x, y: x+y, lambda x, y: x+y )

    def _read_from_topic(self):

        for msg in self.__listener:
            logger.debug('Message received: {0}'.format(msg.value))
            if msg.value == b'POSITION_1':
                self.add_position(msg)
            elif msg.value == b'MARKET_EVENT_1':
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
