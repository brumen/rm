# concrete implementation of the controller.

import datetime
import logging

from time      import sleep
from typing    import List, Tuple
from pyspark   import SparkContext
from threading import Thread
from kafka     import KafkaConsumer, KafkaProducer

from rm.controller2       import Controller

from ao.air_option  import AirOptionMock


logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')


class ControllerJoke(Controller):
    """ Specification for the workers.
    """

    def __init__(self):
        """
        """

        super().__init__(self._value_portfolio_fct)

    def _get_total_current_portfolio(self) -> List:
        return ['POSITION1'] * 100

    def _value_portfolio_fct(self, new_trades : List):
        """ Defines the portfolio_function from trades -> results.

        :returns: results of computation of the portfolio_function of these new trades.
        """

        return len(new_trades)


class ControllerAO(Controller):
    """ Controller for AirOptions.
    """

    def __init__( self
                , server_name         : str = 'localhost'
                , port                : int = 9092
                , topic_to_read_from  : str = 'quickstart-events'
                , topic_to_publish_to : str = 'quickstart-events' ):
        """ Initiates the Controller for computing the AirOptions portfolio.

        :param server_name: kafka server name.
        :param port: port for the kafka server.
        :param topic_to_read_from: topic on Kafka to read from market/trade events.
        :param topic_to_publish_to: topic on kafka server to publish market results to.
        """

        self._topic_to_read_from  = topic_to_read_from
        self._topic_to_publish_to = topic_to_publish_to

        self.__listener = KafkaConsumer(topic_to_read_from, bootstrap_servers = '{0}:{1}'.format(server_name, port))
        self.__reporter = KafkaProducer(bootstrap_servers = '{0}:{1}'.format(server_name, port))  # reports the market to.

        super().__init__(self._value_portfolio_fct)

        # cached values
        self.__sc = None  # spark context

    @property
    def sc(self) -> SparkContext:
        """ Spark context definition.

        :returns: appropriate spark context.
        """

        if self.__sc:
            return self.__sc

        self.__sc = SparkContext()
        return self.__sc

    def _get_total_current_portfolio(self) -> List:
        return ['POSITION1'] * 100

    @staticmethod
    def _value_trade(trade):
        """ Returns the value of the Mock Air Option trade.

        :param trade: trade identifier.
        :returns: value of the trade considered.
        """

        air_option = AirOptionMock( datetime.date(2019, 7, 2)
                                  , origin = 'SFO'
                                  , dest = 'EWR'
                                  , K = 1600.).PV()

        return air_option

    def _value_portfolio_fct(self, new_trades):
        return self._value_portfolio_fct_local(new_trades)

    def _value_portfolio_fct_local(self, new_trades):
        """ Defines the portfolio_function from trades -> results.

        :return:
        """

        return sum([self.__class__._value_trade(trade) for trade in new_trades])

    def _value_portfolio_fct_spark(self, new_trades):
        """ Defines the portfolio_function from trades -> results.

        :return:
        """

        return self.sc.parallelize(new_trades)\
                      .map(self.__class__._value_trade)\
                      .aggregate(0., lambda x, y: x+y, lambda x, y: x+y)

    def _read_from_topic(self):

        for msg in self.__listener:
            if msg.value == b'POSITION_1':
                self.add_position(msg)
            elif msg.value == b'MARKET_EVENT_1':
                self.add_market(msg)

    def _report_results(self, sleep_delay : float = 0.1):
        """ Function that publishes the current market results to Kafka broker.

        :param sleep_delay: delay between individual reportings of the current market results.
        :returns: none, reports to Kafka.
        """

        while True:
            curr_market = str.encode(str(self.curr_market))
            logger.info('Publishing curr_market: {0}'.format(self.curr_market))
            self.__reporter.send(topic=self._topic_to_publish_to, value=curr_market)  # TODO: THIS IS TO BE WORKED UPON.
            sleep(sleep_delay)

    def start(self, idle_delay : float = 0.1) -> Tuple[Thread, Thread, Thread]:
        """ Run the controller.

        :param idle_delay: delay of the IDLE state of the controller.
        :returns: runs all the threads of the controller and returns the thread handles.
        """

        listener_thread = Thread(target=self._read_from_topic, daemon=True)  # listener thread.
        listener_thread.start()
        reporter_thread = Thread(target=self._report_results, daemon=True)  # publisher thread.
        reporter_thread.start()
        controller_thread = super().start(idle_delay)  # start main controller thread.

        return controller_thread, reporter_thread, listener_thread
