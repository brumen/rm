# concrete implementation of the controller, used

import datetime
import logging

import numpy as np

from json      import loads
from uuid      import uuid4
from time      import sleep
from typing    import List, Tuple
from pyspark   import SparkContext, SparkConf
from threading import Thread
from kafka     import KafkaConsumer, KafkaProducer, TopicPartition

from rm.controller2 import Controller
from ao.air_option  import AirOptionMock, AirOptionFlightsFromDB, AirOptionFlightsExplicit
from ao.flight      import AOTrade, DEFAULT_SESSION, Flight, create_session

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class ControllerAO(Controller):
    """ Controller for AirOptions.
    """

    def __init__( self
                , mkt_date            : datetime.date = datetime.date(2016, 1, 1)
                , server_name         : str = 'localhost'
                , port                : int = 9092
                , topic_to_read_from  : str = 'quickstart-events'
                , positions_topic     : str = 'demo.ao.option_positions'
                , topic_to_publish_to : str = 'ao_results' ):
        """ Initiates the Controller for computing the AirOptions portfolio.

        :param mkt_date: market date.
        :param server_name: kafka server name.
        :param port: port for the kafka server.
        :param topic_to_read_from: topic on Kafka to read from market/trade events.
        :param positions_topic: topic to read from positions
        :param topic_to_publish_to: topic on kafka server to publish market results to.
        """

        super().__init__(self._value_portfolio_fct_local, self._value_portfolio_fct_spark)

        self.mkt_date    = mkt_date
        self.server_name = server_name
        self.port        = port

        self._topic_to_read_from  = topic_to_read_from
        self.__positions_topic    = positions_topic
        self._topic_to_publish_to = topic_to_publish_to

        # position listener seeks to beginning
        self.__pos_listener = KafkaConsumer( bootstrap_servers = '{0}:{1}'.format(server_name, port) )  # position listener
        self.__pos_listener.assign([TopicPartition(topic=self.__positions_topic, partition=0)])
        self.__pos_listener.seek_to_beginning()

        self.__mkt_listener = KafkaConsumer(topic_to_read_from, bootstrap_servers = '{0}:{1}'.format(server_name, port))  # market listener TODO: FIX THESE NAMING STUFF
        self.__reporter = KafkaProducer(bootstrap_servers = '{0}:{1}'.format(server_name, port))  # reports the market to.

        # cached values
        self.__sc = None  # spark context
        self.__portfolio = []  # empty portfolio so far, type = List[int]

    @property
    def sc(self) -> SparkContext:
        """ Spark context definition.

        :returns: appropriate spark context.
        """

        if self.__sc:
            return self.__sc

        spark_conf = SparkConf()
        self.__sc = SparkContext.getOrCreate(spark_conf)
        self.__sc.addPyFile(r'/home/brumen/work/work_ao.zip')  # files to be added which contain relevant code.
        return self.__sc

    @property
    def __db_session(self):
        """ Returns the default sqlalchemy session.

        :returns: sqlalchemy session to use.
        """

        return DEFAULT_SESSION

    def _get_total_current_portfolio(self) -> List:
        """ Returns the total portfolio of trades in the air option database.

        :returns: list of trades TODO: DESCRIBE BETTER HERE.
        """

        return self.__portfolio
        # return self.__db_session.query(AOTrade).all()

    def _get_total_current_portfolio_2(self) -> List[AOTrade]:
        """ Returns the total portfolio of trades in the air option database.

        :returns: list of trades TODO: DESCRIBE BETTER HERE.
        """

        return self.__db_session.query(AOTrade).all()

    def _portfolio_worker_function(self) -> None:
        """ Gets all the positions which are in the Kafka queue in self.__listener.
        Kafka has to be set so that the positions are

        :returns: list of current total positions.
        """

        for msg in self.__pos_listener:
            msg_decoded = loads(msg.value.decode())
            # TODO: MISSING WHAT IF IT'S A REMOVAL ???
            self.__portfolio.append(msg_decoded['payload']['after']['position_id'])

    def _value_portfolio_fct_local(self, new_trades : List[int]) -> float:
        """ Defines the portfolio_function from trades -> results.

        :param new_trades: trades to evaluate, given as a list of position numbers.
        :returns: value of the new_trades.
        """

        new_session = create_session()  # use a new session, default session might be in usage.

        return sum([AirOptionFlightsFromDB(datetime.date(2016, 1, 1), trade_nb, session=new_session).PV()
                    for trade_nb in new_trades])

    @staticmethod
    def _value_trade(trade_nb : int) -> float:
        """ Returns the value of the Mock Air Option trade.

        :param trade_nb: trade number to be priced
        :returns: value of the trade considered.
        """

        # TODO: MARKET DATE HAS TO BE FLEXIBLE, NOT HARDCODED.
        # TODO: ALSO SESSION SHOULD POSSIBLY BE passed
        air_option = AirOptionFlightsFromDB( datetime.date(2016, 1, 1), trade_nb).PV()

        return air_option # + np.random.random() * 10.

    def _value_portfolio_fct_spark(self, new_trades : List) -> float:
        """ Defines the portfolio_function from trades -> results.

        :param new_trades: trades to evaluate.
        :returns: value of new_trades.
        """

        return self.sc.parallelize(new_trades)\
                      .map(self.__class__._value_trade)\
                      .aggregate(0., lambda x, y: x+y, lambda x, y: x+y)

    def _read_mkt_events(self):
        """ Reading from listener about market and positions messages and adding them to processing queues.
            Positions are identified as POSITION_1 (TO BE CHANGED)
            Market is identified as MARKET_EVENT_1 (TO BE CHANGED
        :returns: None
        """

        # TODO: Read Market events.
        for msg in self.__mkt_listener:
            if msg.value == b'MARKET_EVENT_1':
                logger.debug('New market event')
                self.add_market(msg)

    def _report_results(self, sleep_delay : float = 0.1):
        """ Function that publishes the current market results to Kafka broker.

        :param sleep_delay: delay between individual reporting of the current market results.
        :returns: None, reports to Kafka.
        """

        while True:
            logger.info('Publishing curr_market: {0}'.format(self.curr_market))
            logger.info('Publishing new_market: {0}'.format(self.new_market))
            logger.info('Current portfolio size: {0}'.format(len(self.__portfolio)))
            self.__reporter.send(topic=self._topic_to_publish_to, value=str.encode(str(self.curr_market)))
            sleep(sleep_delay)

    def start( self
             , controller_delay : float = 0.1
             , report_delay     : float = 0.5 ) -> Tuple[Thread, Thread, Thread, Thread, Thread]:
        """ Run the controller.

        :param controller_delay: delay of the IDLE state of the controller.
        :param report_delay: delay for the reporting
        :returns: runs all the threads of the controller and returns the thread handles.
        """

        market_thread = Thread(target=self._read_mkt_events, daemon=True)  # market event topic reading thread
        market_thread.start()
        position_thread = Thread(target=self._portfolio_worker_function, daemon=True)  # market event topic reading thread
        position_thread.start()
        reporter_thread = Thread(target= lambda : self._report_results(sleep_delay=report_delay), daemon=True)  # publisher thread.
        reporter_thread.start()
        curr_mkt_thread, new_mkt_thread = super().start(controller_delay)  # start main controller thread.

        return curr_mkt_thread, new_mkt_thread, reporter_thread, market_thread, position_thread


# sample start of the controller
# controller = ControllerAO()
# controller.start()
