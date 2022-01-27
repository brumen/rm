""" AO implementation of the controller.
"""

import sys
sys.path.append('/home/brumen/work/')
import datetime
import logging
import requests

from json      import dumps, loads
from time      import sleep
from typing    import List, Tuple, Optional, Union, Any, Dict, Generator
from pyspark   import SparkContext, SparkConf
from threading import Thread
from kafka     import KafkaConsumer, KafkaProducer, TopicPartition

from rm.controller2    import Controller
from ao.air_option     import AirOptionFlights
from ao.trade          import AOTrade, create_session, AOTradeException
from rm.market_service import AOMarketService

logging.basicConfig(filename='/tmp/controller.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


RES_TYPE = Dict[str, Any]


def get_trade(trade_id : int, db_session = None) -> Union[None, AOTrade]:
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


class ControllerAO(Controller):
    """ Controller for AirOptions.
    """

    LOCAL_WORK_LIMIT = 50  # when to switch to spark

    def __init__( self
                , mkt_date        : datetime.date = datetime.date(2016, 1, 1)
                , server_name     : str  = 'localhost'
                , port            : int  = 9092
                , mkt_topic       : str  = 'mkt_events'
                , mkt_rester      : str  = 'http://localhost:5000/mkt/get_market'
                , positions_topic : str  = 'air_options.ao.option_positions'
                , results_topic   : str  = 'ao_results'
                , spark_ctx       : Dict = {'pyfile': r'/home/brumen/work/work_ao.zip' } ):
        """ Initiates the Controller for computing the AirOptions portfolio.

        :param mkt_date: market date.
        :param server_name: kafka server name.
        :param port: port for the kafka server.
        :param mkt_topic: topic on Kafka to read from market/trade events.
        :param positions_topic: topic to read from positions.
        :param results_topic: topic where results are published.
        :param spark_ctx: configuration of spark context.
        """

        self.mkt_date    = mkt_date
        self.server_name = server_name
        self.port        = port

        self._mkt_topic     = mkt_topic
        self._mkt_rester    = mkt_rester
        self._results_topic = results_topic
        self._positions_topic = positions_topic

        super().__init__()  # _mkt_rester should be defined.

        bootstrap_servers = f'{server_name}:{port}'

        self.__mkt_listener = KafkaConsumer(mkt_topic, bootstrap_servers = bootstrap_servers)
        self.__reporter     = KafkaProducer(bootstrap_servers = bootstrap_servers)  # reports the market to.

        self.__position_listener = KafkaConsumer(bootstrap_servers = bootstrap_servers)
        self.__position_listener.assign([TopicPartition(topic=positions_topic, partition=0)])
        self.__position_listener.seek_to_beginning()

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

    @classmethod
    def _value_trade(cls, mkt_date_trade: Tuple[datetime.date, Optional[AOTrade], str], params) -> Union[None, RES_TYPE]:
        """ Returns the value of the Mock Air Option trade.

        :param mkt_date_trade: tuple of market date and AOTrade., and trade direction. 'c', 'd'
        :returns: value of the trade considered.
        """

        mkt_date, ao_trade, trade_direction = mkt_date_trade

        if ao_trade is None:
            return None

        # ao_trade is not None, price.
        try:
            return cls._compute_trade(mkt_date, ao_trade, trade_direction, params)

        except AOTradeException:  # fails in AOTrade
            logger.error(f'Trade {ao_trade.position_id} could not be found in the database.')
            return None

        except Exception as e:
            logger.error(f'Trade {ao_trade.position_id} could not be priced. Reason: {str(e)}')
            return None

    @classmethod
    def _compute_trade(cls, mkt_date : datetime.date, ao_trade : AOTrade, trade_direction : str, params) -> RES_TYPE:

        old_style = False

        if old_style:
            return cls._compute_trade_on_the_fly(mkt_date, ao_trade, trade_direction)

        return cls._compute_trade_from_mkt(mkt_date, ao_trade, trade_direction, params)  # TODO: here the parameters are market

    @classmethod
    def _compute_trade_on_the_fly(cls, mkt_date : datetime.date, ao_trade : AOTrade, trade_direction : str) -> RES_TYPE:
        """ Raw computation of the trade.

        :param mkt_date: market date
        :param ao_trade: ao trade to be values.
        :param trade_direction: direction of the trade, 'c' for long, 'd' for short
        :returns: value that should be computed
        """

        # TODO: params usage

        # OLD USAGE:
        aof = AirOptionFlights.from_flights( mkt_date, ao_trade.flights, ao_trade.strike)

        pv = aof.PV()
        pv01 = aof.PV01()
        logger.debug(f'PV, PV01 of {ao_trade}: {pv, pv01}')

        return { 'PV'  : pv if trade_direction == 'c' else - pv
               , 'PV01': pv01 if trade_direction == 'c' else - pv01
               , }

    @classmethod
    def _compute_trade_from_mkt( cls
                               , mkt_date        : datetime.date
                               , ao_trade        : AOTrade
                               , trade_direction : str
                               , market          : Dict[Tuple[str, datetime.date], float]
                               , default_price   : float = 200.
                               , ) -> RES_TYPE:
        """ Computes the trade from the market provided (market).

        :param mkt_date: market date
        :param ao_trade: ao trade to be values.
        :param trade_direction: direction of the trade, 'c' for long, 'd' for short
        :param market : market provided
        :param default_price: default price if the flight could not be found in the market
        :returns: value that should be computed
        """

        flights = []
        for flight in ao_trade.flights:
            # flight has the following attributes
            # flight_id = Column(Integer, primary_key=True)
            # flight_id_long = Column(String)
            # orig = Column(String)
            # dest = Column(String)
            # dep_date = Column(DateTime)
            # arr_date = Column(DateTime)
            # carrier = Column(String)

            dep_date  = flight.dep_date.date()  # this is datetime.datetime by default
            flight_id = flight.flight_id  # TODO: CHECK IF THIS IS TRUE
            carrier   = flight.carrier
            flight_nb = f'{carrier}{flight_id}'

            mkt_price = market.get((flight_nb, dep_date))
            if mkt_price is None:  # if market doesnt contain price
                found_prices = flight.prices  # prices found in the database
                # find the last price, otherwise report a random price
                mkt_price = found_prices[-1].price if found_prices else default_price

            flights.append((mkt_price, dep_date, flight_nb))

        aof = AirOptionFlights( mkt_date, flights, ao_trade.strike)

        pv = aof.PV()
        pv01 = aof.PV01()

        return { 'PV'  : pv if trade_direction == 'c' else - pv
               , 'PV01': pv01 if trade_direction == 'c' else - pv01
               , }

    @staticmethod
    def _trade_result_agg_single(trade_pv_1 : Optional[RES_TYPE], trade_pv_2 : Optional[RES_TYPE]) -> Union[None, RES_TYPE]:
        """ Aggregates two Dict[str, Any]"""

        if trade_pv_1 is None:
            if trade_pv_2 is None:
                return None

            return trade_pv_2

        if trade_pv_2 is None:  # trade_pv_1 is not None
            return trade_pv_1

        # neither is none
        calcs_1 = list(trade_pv_1.keys())  # calcs are calculators, like PV, PV01...
        assert calcs_1 == list(trade_pv_2.keys()), f'Calculator results dont have the same calculators.'  # not same calcs

        for calc in calcs_1:
            trade_pv_1[calc] += trade_pv_2[calc]  # each calc supports aggregation +

        return trade_pv_1

    @classmethod
    def _value_trade_id( cls
                       , mkt_date_trade_id : Tuple[datetime.date, Tuple[int, str]]
                       , db_session = None
                       , params = None ) -> Union[None, RES_TYPE]:
        """ Returns the PV of the trade with trade_id.

        :param mkt_date_trade_id: market date and trade id as a tuple (useful for spark calculations)
        :param db_session: sql alchemy session.
        :param params: parameters passed, mostly market information
        :returns: PV of the referenced trade.
        """

        mkt_date, (trade_id, trade_direction) = mkt_date_trade_id

        return cls._value_trade((mkt_date, get_trade(trade_id, db_session), trade_direction), params)

    def _value_portfolio_local( self
                              , trade_ids : Union[List[Tuple[int, str]], Generator[Tuple[int, str], None, None]]
                              , params
                              , ) -> Generator[RES_TYPE, None, None]:
        """ Defines the portfolio_function from trades -> results.

        :param trade_ids: trade ids to evaluate, given as a list of position numbers.
                       (tuple of (trade_id, 'c') or (trade_id, 'd')
                       'c' means creating trade, 'd' means deleting trade
        :returns: value of the new_trades.
        """

        db_sess = create_session()

        for trade_nb, trade_id in enumerate(trade_ids):
            logger.debug(f'Valuing trade {trade_nb}.')
            yield self.__class__._value_trade_id((self.mkt_date, trade_id), db_session=db_sess, params=params)

    def _value_portfolio_remote( self
                               , trade_ids : Union[List[Tuple[int, str]], Generator[Tuple[int, str], None, None]]
                               , ) -> List[RES_TYPE]:
        """ Defines the portfolio_function from trades -> results.

        :param trade_ids: trades to evaluate.
        :returns: value of new_trades.
        """

        trade_ids_l = list(trade_ids)  # TODO: THIS SHOULD BE BETTER

        trades = zip( [self.mkt_date] * len(trade_ids_l), trade_ids_l )

        return self.sc.parallelize(trades)\
                      .map(self.__class__._value_trade_id)\
                      .collect()
                      # .aggregate(0., self.__class__._trade_result_agg, self.__class__._trade_result_agg)

    def _snap_market(self) -> Dict[Tuple[str, datetime.date], float]:
        """ Snaps the latest market from the rester service.

        :returns Dict[Tuple[str, datetime.date], float]: market w/ flights and date as keys, flight prices as
             values.
        """

        response = requests.get(self._mkt_rester)
        market_id, market = AOMarketService.decode_mkt(response.json())

        return market

    def __construct_portfolio(self) -> None:
        """ Gets all the positions which are in the Kafka queue in self.__listener
               and saves them to self.__portfolio.

        :returns: None, only the
        """

        for msg in self.__position_listener:
            logger.debug(msg)
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
                self.__reporter.send(topic=self._results_topic, value=str.encode(dumps(field_value)))

            sleep(sleep_delay)

    def _handle_mkt_events(self) -> None:
        """ Handles market events.

        :returns: nothing, just handles the signals
        """

        for msg in self.__mkt_listener:
            logger.debug(msg)
            if msg.value is None:  # TODO: CHECK THIS HERE!!!
                continue

            self.new_mkt_event()

    def start( self
             , controller_delay : float = 0.3
             , report_delay     : float = 0.5 ) -> List[Thread]:
        """ Run the controller.

        :param controller_delay: delay of the IDLE state of the controller.
        :param report_delay: delay for the reporting
        :returns: runs all the threads of the controller and returns the thread handles.
        """

        controller_threads = super().start(controller_delay)  # start main controller thread.

        # gets the positions from kafka
        position_thread = Thread(target=self.__construct_portfolio, daemon=True)  # positions reading thread
        position_thread.start()

        # gets the market positions from Kafka
        market_events_thread = Thread(target=self._handle_mkt_events, daemon=True)
        market_events_thread.start()

        # reports results
        reporter_thread = Thread(target= lambda : self._report_results(sleep_delay=report_delay), daemon=True)  # publisher thread.
        reporter_thread.start()

        # curr_mkt_thread computes current market, new_mkt_thread is computing new market
        controller_threads.extend([position_thread, reporter_thread, market_events_thread])

        return controller_threads
