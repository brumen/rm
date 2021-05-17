# concrete implementation of the controller, used

import datetime
import logging

from json      import loads, dumps
from time      import sleep
from typing    import List, Tuple, Optional
from pyspark   import SparkContext, SparkConf
from threading import Thread
from kafka     import KafkaConsumer, KafkaProducer, TopicPartition

from rm.controller2 import Controller
from ao.air_option  import AirOptionFlightsFromDB, AOTradeException, AirOptionFlightsExplicit
from ao.flight      import AOTrade, create_session

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

        bootstrap_servers = f'{server_name}:{port}'
        # position listener seeks to beginning
        self.__pos_listener = KafkaConsumer( bootstrap_servers = bootstrap_servers )
        self.__pos_listener.assign([TopicPartition(topic=self.__positions_topic, partition=0)])
        self.__pos_listener.seek_to_beginning()  # start reading positions from beginning

        self.__mkt_listener = KafkaConsumer(topic_to_read_from, bootstrap_servers = bootstrap_servers)
        self.__reporter = KafkaProducer(bootstrap_servers = bootstrap_servers)  # reports the market to.

        self.__spark_ctx = spark_ctx  # spark context config

        # cached values
        self.__sc = None  # spark context
        self.__current_sqlalchemy_session = None

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
        if 'pyfile' in self.__spark_ctx:  # files to be added which contain relevant code
            self.__sc.addPyFile(self.__spark_ctx['pyfile'])

        return self.__sc

    @staticmethod
    def __retrieve_tradeao(trade_id : int, db_session = None) -> AOTrade:
        """ Returns the trade corresponding to this trade_id in the air option database.

        :returns: trade requested.
        """

        db_sess_used = db_session if db_session is not None else create_session()
        ao_trade = db_sess_used.query(AOTrade).filter_by(position_id=trade_id).first()

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
            self.add_position([trade_id])

    @staticmethod
    def _value_trade(mkt_date_trade: Tuple[datetime.date, AOTrade]) -> Optional[float]:
        """ Returns the value of the Mock Air Option trade.

        :param mkt_date_trade: tuple of market date and AOTrade.
        :returns: value of the trade considered.
        """

        mkt_date, ao_trade = mkt_date_trade

        try:
            return AirOptionFlightsExplicit( mkt_date, ao_trade.flights, ao_trade.strike).PV()
            # return AirOptionFlightsFromDB(mkt_date, trade_nb).PV()

        except AOTradeException:  # fails in AOTrade
            logger.error(f'Trade {ao_trade.position_id} could not be found in the database.')
            return None

        except Exception as e:
            logger.error(f'Trade {ao_trade.position_id} could not be priced. Reason: {str(e)}')
            return None

    @staticmethod
    def _value_trade_id(mkt_date_trade_id : Tuple[datetime.date, int], db_session = None) -> float:
        """ Returns the PV of the trade with trade_id.

        :param mkt_date_trade_id: market date and trade id as a tuple (useful for spark calculations)
        :param db_session: sql alchemy session.
        :returns: PV of the referenced trade.
        """
        mkt_date, trade_id = mkt_date_trade_id

        return ControllerAO._value_trade((mkt_date, ControllerAO.__retrieve_tradeao(trade_id, db_session)))

    def _value_portfolio_fct_local(self, trade_ids : List[int]) -> List[float]:
        """ Defines the portfolio_function from trades -> results.

        :param trade_ids: trade ids to evaluate, given as a list of position numbers.
        :returns: value of the new_trades.
        """

        # trade results - either float or None
        db_sess = create_session()

        return [ self.__class__._value_trade_id((self.mkt_date, trade_id), db_session=db_sess)
                 for trade_id in trade_ids]

    def _value_portfolio_fct_spark(self, trade_ids : List[int]) -> List[float]:
        """ Defines the portfolio_function from trades -> results.

        :param trade_ids: trades to evaluate.
        :returns: value of new_trades.
        """

        nb_new_trades = len(trade_ids)

        trades = list(zip( [self.mkt_date] * nb_new_trades, trade_ids ))  # zip makes a generator, it has to be evaluated, BUMMER

        return self.sc.parallelize(trades)\
                      .map(self.__class__._value_trade_id)\
                      .collect()
                      # .aggregate(0., self.__class__._trade_result_agg, self.__class__._trade_result_agg)

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
        :returns: None, reports current market results to Kafka topic on self.__reporter.
        """

        while True:
            logger.debug(f'Value of curr_market: {self.curr_market}')
            logger.debug(f'Value of new_market: {self.new_market}')
            logger.debug(f'Current portfolio size: {len(self.all_trades)}')

            for field_value in [ ('curr_market', self.curr_market)
                               , ('new_market', self.new_market)
                               , ('curr_trades', self.curr_mkt_queue_size())
                               , ('new_trades', self.new_mkt_queue_size())
                               , ]:
                self.__reporter.send(topic=self._topic_to_publish_to, value=str.encode(dumps(field_value)))

            sleep(sleep_delay)

    def start( self
             , controller_delay : float = 0.3
             , report_delay     : float = 0.5 ) -> List[Thread]:
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
        controller_threads = super().start(controller_delay)  # start main controller thread.
        controller_threads.extend([market_events_thread, position_thread, reporter_thread])

        return controller_threads


# sample start of the controller
controller = ControllerAO()
controller.start()
