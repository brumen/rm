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
from ao.air_option  import AirOptionFlights
from ao.trade       import AOTrade, create_session, AOTradeException

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
                , mkt_topic           : str  = 'market.events'
                , positions_topic     : str  = 'air_options.ao.option_positions'
                , topic_to_publish_to : str  = 'ao_results'
                , spark_ctx           : dict = {'pyfile': r'/home/brumen/work/work_ao.zip' } ):
        """ Initiates the Controller for computing the AirOptions portfolio.

        :param mkt_date: market date.
        :param server_name: kafka server name.
        :param port: port for the kafka server.
        :param mkt_topic: topic on Kafka to read from market/trade events.
        :param positions_topic: topic to read from positions
        :param topic_to_publish_to: topic on kafka server to publish market results to.
        :param spark_ctx: configuration of spark context.
        """

        super().__init__()

        self.mkt_date    = mkt_date
        self.server_name = server_name
        self.port        = port

        self._mkt_topic           = mkt_topic
        self.__positions_topic    = positions_topic
        self._topic_to_publish_to = topic_to_publish_to

        bootstrap_servers = f'{server_name}:{port}'
        # position listener seeks to beginning
        self.__pos_listener = KafkaConsumer( bootstrap_servers = bootstrap_servers )
        self.__pos_listener.assign([TopicPartition(topic=self.__positions_topic, partition=0)])
        self.__pos_listener.seek_to_beginning()  # start reading positions from beginning

        self.__mkt_listener = KafkaConsumer(mkt_topic, bootstrap_servers = bootstrap_servers)
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
    def _retrieve_tradeao(trade_id : int, db_session = None) -> Optional[AOTrade]:
        """ Returns the trade corresponding to this trade_id in the air option database.

        :returns: trade requested.
        """

        db_sess_used = db_session if db_session is not None else create_session()
        ao_trade = db_sess_used.query(AOTrade).filter_by(position_id=trade_id).first()

        if ao_trade is None:  # trade was deleted, assign 0 to that trade.
            logger.info(f'Trade {trade_id} was attempted to retrieve, unable. Returning 0.')
            return None

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
            if msg.value is None:  # TODO: CHECK THIS HERE!!!
                continue

            msg_decoded = loads(msg.value.decode())
            msg_payload = msg_decoded['payload']

            # create events
            event_type = msg_payload['op']  # either c - create, d - delete, u - update
            if event_type == 'c':  # create event
                trade_id = msg_payload['after']['position_id']  # adding this position id
                self.add_position([(trade_id, 'c')])  # 'c' for create, 'd' for delete

            elif event_type == 'd':  # deleting the trade
                trade_id = msg_payload['before']['position_id']
                self.add_position([(trade_id, 'd')])

            elif event_type == 'u':  # updating the trade
                raise NotImplementedError('Updating of trades not yet implemented.')
                # trade_id_before = msg_payload['before']['position_id']
                # trade_id_after  = msg_payload['after']['position_id']
                # assert trade_id_before == trade_id_after, f'Updated position nbs differ: {trade_id_before}, {trade_id_after}.'
                # pass

            else:  # unknown type of event, raise RuntTimeError
                raise RuntimeError(f'Unknown event type: {event_type}')

    @classmethod
    def _value_trade(cls, mkt_date_trade: Tuple[datetime.date, Optional[AOTrade]]) -> Optional[float]:
        """ Returns the value of the Mock Air Option trade.

        :param mkt_date_trade: tuple of market date and AOTrade.
        :returns: value of the trade considered.
        """

        mkt_date, ao_trade = mkt_date_trade

        if ao_trade is None:
            return 0.

        # ao_trade is not None, price.
        try:
            return cls._compute_trade(mkt_date, ao_trade)

        except AOTradeException:  # fails in AOTrade
            logger.error(f'Trade {ao_trade.position_id} could not be found in the database.')
            return None

        except Exception as e:
            logger.error(f'Trade {ao_trade.position_id} could not be priced. Reason: {str(e)}')
            return None

    @classmethod
    def _compute_trade(cls, mkt_date : datetime.date, ao_trade : AOTrade):
        """ Raw computation of the trade.

        :param mkt_date: market date
        :param ao_trade: ao trade to be values.
        :returns: value that should be computed
        """

        return AirOptionFlights.from_flights( mkt_date, ao_trade.flights, ao_trade.strike).PV()

    @classmethod
    def _value_trade_id(cls, mkt_date_trade_id : Tuple[datetime.date, Tuple[int, str]], db_session = None) -> float:
        """ Returns the PV of the trade with trade_id.

        :param mkt_date_trade_id: market date and trade id as a tuple (useful for spark calculations)
        :param db_session: sql alchemy session.
        :returns: PV of the referenced trade.
        """
        mkt_date, (trade_id, trade_direction) = mkt_date_trade_id

        trade_value = cls._value_trade((mkt_date, cls._retrieve_tradeao(trade_id, db_session)))

        return trade_value if trade_direction == 'c' else - trade_value

        # if trade_direction == 'd':  # deleted trade
        #    return - trade_value
        # raise RuntimeError(f'Unable to handle trade {trade_id} for valuation')

    def _value_portfolio_local(self, trade_ids : List[Tuple[int, str]]) -> List[float]:
        """ Defines the portfolio_function from trades -> results.

        :param trade_ids: trade ids to evaluate, given as a list of position numbers.
                       (tuple of (trade_id, 'c') or (trade_id, 'd')
                       'c' means creating trade, 'd' means deleting trade
        :returns: value of the new_trades.
        """

        # trade results - either float or None
        db_sess = create_session()

        return [ self.__class__._value_trade_id((self.mkt_date, trade_id), db_session=db_sess)
                 for trade_id in trade_ids]

    def _value_portfolio_remote(self, trade_ids : List[Tuple[int, str]]) -> List[float]:
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


def main():
    # sample start of the controller
    controller = ControllerAO()
    controller.start()

# main()
