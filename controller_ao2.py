# concrete implementation of the controller, used

import datetime
import logging

from json      import loads
from time      import sleep
from typing    import List, Tuple, Optional
from pyspark   import SparkContext, SparkConf
from threading import Thread
from kafka     import KafkaConsumer, KafkaProducer, TopicPartition

from rm.controller2 import Controller
from ao.air_option  import AirOptionFlightsFromDB, AOTradeException, AirOptionFlightsExplicit
from ao.flight      import AOTrade, DEFAULT_SESSION, create_session

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class ControllerAO(Controller):
    """ Controller for AirOptions.
    """

    def __init__( self
                , mkt_date            : datetime.date = datetime.date(2016, 1, 1)
                , server_name         : str  = 'localhost'
                , port                : int  = 9092
                , topic_to_read_from  : str  = 'quickstart-events'
                , positions_topic     : str  = 'demo.ao.option_positions'
                , topic_to_publish_to : str  = 'ao_results'
                , spark_ctx           : dict = {'pyfile': r'/home/brumen/work/work_ao.zip' } ):
        """ Initiates the Controller for computing the AirOptions portfolio.

        :param mkt_date: market date.
        :param server_name: kafka server name.
        :param port: port for the kafka server.
        :param topic_to_read_from: topic on Kafka to read from market/trade events.
        :param positions_topic: topic to read from positions
        :param topic_to_publish_to: topic on kafka server to publish market results to.
        :param spark_ctx: configuration of spark context.
        """

        super().__init__(self._value_portfolio_fct_local, self._value_portfolio_fct_spark)

        self.mkt_date    = mkt_date
        self.server_name = server_name
        self.port        = port

        self._topic_to_read_from  = topic_to_read_from
        self.__positions_topic    = positions_topic
        self._topic_to_publish_to = topic_to_publish_to

        # position listener seeks to beginning
        self.__pos_listener = KafkaConsumer( bootstrap_servers = '{0}:{1}'.format(server_name, port) )
        self.__pos_listener.assign([TopicPartition(topic=self.__positions_topic, partition=0)])
        self.__pos_listener.seek_to_beginning()  # start reading positions from beginning

        self.__mkt_listener = KafkaConsumer(topic_to_read_from, bootstrap_servers = '{0}:{1}'.format(server_name, port))  # market listener TODO: FIX THESE NAMING STUFF
        self.__reporter = KafkaProducer(bootstrap_servers = '{0}:{1}'.format(server_name, port))  # reports the market to.

        self.__spark_ctx = spark_ctx  # spark context config

        # cached values
        self.__sc = None  # spark context
        self.__portfolio_trade_ids = []  # empty portfolio so far, type = List[int]
        self.__portfolio_tradeAo   = []  # empty portfolio of List[AOTrade]
        self.__current_sqlalchemy_session = DEFAULT_SESSION

    @property
    def sc(self) -> SparkContext:
        """ Spark context definition.

        :returns: appropriate spark context.
        """

        if self.__sc:
            return self.__sc

        # spark configuration
        spark_conf = SparkConf().setMaster('local[8]')

        self.__sc = SparkContext.getOrCreate(spark_conf)
        self.__sc.addPyFile(self.__spark_ctx['pyfile'])  # files to be added which contain relevant code.

        return self.__sc

    @property
    def __db_session(self):
        """ Returns the default sqlalchemy session.

        :returns: sqlalchemy session to use.
        """

        if self._new_trade_event:
            self.__current_sqlalchemy_session = create_session()
            self._new_trade_event = False  # setting the trade event back to False

        return self.__current_sqlalchemy_session

    def _get_total_current_portfolio(self) -> List[AOTrade]:
        """ Stored portfolio of all trades, for faster access.

        :returns: list of current portfolio of realized AOTrade trades.
        """

        return self.__portfolio_tradeAo

    def __retrieve_tradeao(self, trade_id : int) -> AOTrade:
        """ Returns the trade corresponding to this trade_id in the air option database.

        :returns: trade requested.
        """

        ao_trade = self.__db_session.query(AOTrade).filter_by(position_id=trade_id).first()

        if ao_trade is None:
            raise RuntimeError(f'Could not find trade id {trade_id} in the database.')

        # touch so that the stuff reloads  (possibly can be made better)
        x = ao_trade.position_id
        x = ao_trade.flights
        x = ao_trade.strike

        return ao_trade

    def __construct_portfolio(self) -> None:
        """ Gets all the positions which are in the Kafka queue in self.__listener
               and saves them to self.__portfolio.

        :returns: None, only the
        """

        for msg in self.__pos_listener:
            msg_decoded = loads(msg.value.decode())

            # TODO: MISSING A CASE WHEN IT'S A REMOVAL ???
            trade_id = msg_decoded['payload']['after']['position_id']
            self.__portfolio_trade_ids.append(trade_id)
            self.__portfolio_tradeAo.append(self.__retrieve_tradeao(trade_id))

    def _value_portfolio_fct_local(self, new_trades : List[int]) -> float:
        """ Defines the portfolio_function from trades -> results.

        :param new_trades: trades to evaluate, given as a list of position numbers.
        :returns: value of the new_trades.
        """

        # trade results - either float or None
        trade_results = [self.__class__._value_trade((self.mkt_date, trade_nb))
                         for trade_nb in new_trades]

        # TODO: IGNORE None - CHECK IF THIS IS THE DESIRED BEHAVIOR
        return sum([trade_value for trade_value in trade_results if trade_value is not None])

    @staticmethod
    def _value_trade2(mkt_date: datetime.date, trade_nb : int) -> Optional[float]:
        """ Returns the value of the Mock Air Option trade.

        :param mkt_date: market date for the pricing.
        :param trade_nb: trade number to be priced
        :returns: value of the trade considered.
        """

        # TODO: MARKET DATE HAS TO BE FLEXIBLE, NOT HARDCODED.
        try:
            return AirOptionFlightsFromDB( mkt_date, trade_nb).PV()

        except AOTradeException:  # fails in AOTrade
            logger.error(f'Trade {trade_nb} could not be found in the database.')
            return None

        except Exception as e:
            logger.error(f'Trade {trade_nb} could not be priced. Reason: {str(e)}')
            return None

    @staticmethod
    def _value_trade(mkt_date_trade: Tuple[datetime.date, AOTrade]) -> Optional[float]:
        """ Returns the value of the Mock Air Option trade.

        :param mkt_date_trade: tuple of market date and AOTrade.
        :returns: value of the trade considered.
        """

        mkt_date, ao_trade = mkt_date_trade

        # session_used = None if pickled_session is None else dill.loads(pickled_session)

        # TODO: MARKET DATE HAS TO BE FLEXIBLE, NOT HARDCODED.
        try:
            return AirOptionFlightsExplicit( mkt_date, ao_trade.flights, ao_trade.strike).PV()

        except AOTradeException:  # fails in AOTrade
            logger.error(f'Trade {ao_trade.position_id} could not be found in the database.')
            return None

        except Exception as e:
            logger.error(f'Trade {ao_trade.position_id} could not be priced. Reason: {str(e)}')
            return None

    @staticmethod
    def _trade_result_agg(trade_pv_1, trade_pv_2):
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

    def _value_portfolio_fct_spark(self, new_trades : List) -> float:
        """ Defines the portfolio_function from trades -> results.

        :param new_trades: trades to evaluate.
        :returns: value of new_trades.
        """

        nb_new_trades = len(new_trades)

        trades_session = list(zip( [self.mkt_date] * nb_new_trades, new_trades ))

        return self.sc.parallelize(trades_session)\
                      .map(self.__class__._value_trade) \
                      .aggregate(0., self.__class__._trade_result_agg, self.__class__._trade_result_agg)

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
            logger.info('Current portfolio size: {0}'.format(len(self.__portfolio_trade_ids)))
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

        # gets market events from kafka
        market_events_thread = Thread(target=self._read_mkt_events, daemon=True)  # market event topic reading thread
        market_events_thread.start()

        # gets the positions from kafka
        position_thread = Thread(target=self.__construct_portfolio, daemon=True)  # positions reading thread
        position_thread.start()

        # reports results
        reporter_thread = Thread(target= lambda : self._report_results(sleep_delay=report_delay), daemon=True)  # publisher thread.
        reporter_thread.start()

        # curr_mkt_thread computes current market, new_mkt_thread is computing new market
        curr_mkt_thread, new_mkt_thread = super().start(controller_delay)  # start main controller thread.

        return curr_mkt_thread, new_mkt_thread, reporter_thread, market_events_thread, position_thread


# sample start of the controller
# controller = ControllerAO()
# controller.start()
