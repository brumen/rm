""" AO implementation of the controller.
"""

import sys
import datetime
import logging
import copy
import requests

from requests import Response

from json      import dumps, loads
from typing    import List, Tuple, Optional, Union, Any, Dict, Generator
from threading import Thread
from kafka     import KafkaConsumer, TopicPartition, KafkaProducer
from time      import sleep
from yaml      import safe_load

sys.path.append('/home/brumen/work/')

from ao.air_option     import AirOptionFlights
from ao.trade          import AOTrade, create_session, AOTradeException
from rm.controller2    import Controller
from rm.market_service import AOMarketService
from rm.delta_dict     import DeltaDict

logging.basicConfig(filename='/tmp/controller_ao.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


RES_TYPE = Dict[str, Any]  # resulting type


class ControllerAO(Controller):
    """ Controller for AirOptions. This is how it works:

    1. The positions are read from the positions_topic (air_options.ao.option_positions)
    2. market events are read from mkt_events topic. (mkt_events)
    3. holds the market results in the properties curr_market, new_market
    """

    LOCAL_WORK_LIMIT = 50  # when to switch to spark
    VALUE_TRADE      = 'market'  # either 'on-the-fly' or 'market'

    def __init__( self
                , mkt_date        : datetime.date = datetime.date(2016, 1, 1)
                , server_name     : str  = 'localhost'
                , port            : int  = 9092
                , mkt_topic       : str  = 'mkt_events'
                , positions_topic : str  = 'air_options.ao.option_positions'
                , results_topic   : str  = 'air_options.ao.results'
                , trade_pricer    : str  = 'http://localhost:5010'
                , local_only      : bool = False
                , pricing_config: str  = r'/home/brumen/work/rm/configuration.yaml'
                , ):
        """ Initiates the Controller for computing the AirOptions portfolio.

        The controller reacts to two inputs:
            1. market events published on mkt_events topic of the kafka.
            2. position events published on the air_options.ao.option_positions

        :param mkt_date: market date.
        :param server_name: kafka server name.
        :param port: port for the kafka server.
        :param mkt_topic: topic on Kafka to read from market/trade events.
        :param positions_topic: topic to read from positions.
        :param local_only: indicator whether to use only local machine, no Spark
        :param pricing_config: configuration file for the pricing parameters.
        """

        logger.debug(f'Starting logger.')

        self._latest_market = None

        with open(pricing_config) as pricing_file:
             pricing_params = safe_load(pricing_file)

        super().__init__(local_only=local_only, pricing_params=pricing_params)  # _mkt_rester should be defined.

        self.mkt_date    = mkt_date
        self.server_name = server_name
        self.port        = port

        self._mkt_topic     = mkt_topic
        self._positions_topic = positions_topic
        self._results_topic   = results_topic

        self._trade_pricer = trade_pricer

        bootstrap_servers = f'{server_name}:{port}'

        self.__mkt_listener = KafkaConsumer(bootstrap_servers = bootstrap_servers)
        self.__mkt_listener.assign([TopicPartition(topic=mkt_topic, partition=0)])
        self.__mkt_listener.seek_to_beginning()

        self.__position_listener = KafkaConsumer(bootstrap_servers = bootstrap_servers)
        self.__position_listener.assign([TopicPartition(topic=positions_topic, partition=0)])
        self.__position_listener.seek_to_beginning()

        self.__results_publisher = KafkaProducer(bootstrap_servers = bootstrap_servers)


    @classmethod
    def _value_trade(cls
                     , mkt_date_trade: Tuple[datetime.date, Optional[AOTrade], str]
                     , mkt_params    : Dict
                     , ao_params     : Dict[str, Any]) -> Union[None, RES_TYPE]:
        """ Returns the value of the Mock Air Option trade.

        :param mkt_date_trade: tuple of market date and AOTrade., and trade direction. 'c', 'd'
        :param mkt_params: market params
        :param ao_params: paratemers for the valuation/risk of the trade.
        :returns: value of the trade considered.
        """

        mkt_date, ao_trade, trade_direction = mkt_date_trade

        if ao_trade is None:
            return None

        # ao_trade is not None, price.
        try:
            return cls._compute_trade(mkt_date, ao_trade, trade_direction, mkt_params, ao_params)

        except AOTradeException:  # fails in AOTrade
            logger.error(f'Trade {ao_trade.position_id} could not be found in the database.')
            return None

        except Exception as e:
            logger.error(f'Trade {ao_trade.position_id} could not be priced. Reason: {str(e)}')
            return None

    @staticmethod
    def _trade_result_agg_single(trade_pv_1 : Optional[RES_TYPE], trade_pv_2 : Optional[RES_TYPE]) -> Union[None, RES_TYPE]:
        """ Aggregates two dictionaries of type: Dict[str, Any]"""

        if trade_pv_1 is None:
            if trade_pv_2 is None:
                return None

            return trade_pv_2

        if trade_pv_2 is None:  # trade_pv_1 is not None
            return trade_pv_1

        for calc_type, calc_val in trade_pv_1.items():  # calc_type is 'PV', 'PV01', etc.
            if calc_type not in trade_pv_2:
                raise RuntimeError(f'{calc_type} not present in the second computed value.')

            trade_pv_1[calc_type] += trade_pv_2[calc_type]  # each calc needs to support aggregation +

        return trade_pv_1

    def _value_trade_id( self
                       , trade_id_direction : Tuple[int, str]
                       , mkt_params         : Optional[Dict] = None
                       , ao_params          : Optional[Dict[str, Any]] = {} ) -> Union[None, RES_TYPE]:
        """ Returns the PV of the trade with trade_id.

        :param mkt_date_trade_id: market date and trade id as a tuple (useful for spark calculations)
        :param params: parameters passed, mostly market information
        :returns: PV of the referenced trade.
        """

        trade_id, trade_direction = trade_id_direction

        result = requests.get(f'{self._trade_pricer}/pv_mkt/{trade_id}')  # pv on the market

        logger.debug(f'Value of trade {trade_id}: {result.json()}')

        pv = result.json()[str(trade_id)]

        return {'PV': DeltaDict({trade_id: pv}, ),
                }

    def _value_portfolio( self
                          , trade_ids : Union[List[Tuple[int, str]], Generator[Tuple[int, str], None, None]]
                          , mkt_params
                          , ao_params
                          , ) -> Union[List[RES_TYPE], Generator[RES_TYPE, None, None]]:
        """ Defines the portfolio_function from trades -> results.

        :param trade_ids: trade ids to evaluate, given as a list of position numbers.
                       (tuple of (trade_id, 'c') or (trade_id, 'd')
                       'c' means creating trade, 'd' means deleting trade
        :param mkt_params: market params
        :param ao_params: parameters for the valuation/risk of the trade.
        :returns: value of the new_trades.
        """

        # update the market
        # TODO: CONVERSIONS HAVE TO BE DECOUPLED.
        results_mkt = requests.post(f'{self._trade_pricer}/market/', data=self._snap_market())  # update market

        results_mkt_date = requests.post(f'{self._trade_pricer}/market_date/', data=self.mkt_date.strftime("%Y%m%d"))  # update market date

        for trade_nb, trade_id in enumerate(trade_ids):
            logger.debug(f'Valuing trade {trade_nb}.')
            trade_value = self._value_trade_id(trade_id, mkt_params=mkt_params, ao_params=ao_params)
            logger.debug(f'Value of trade {trade_nb}: {trade_value}')
            yield trade_value

    @staticmethod
    def _decode_mkt(request_json):
        """ Decodes the market information, used for pricing the trades.

        :param request_json: market information to be decoded in a recognizable format.
        :returns: market information, in this case of the form
                   Dict[Tuple[str, datetime.date], float]
        """

        return AOMarketService.decode_mkt(request_json)

    def _snap_market(self) -> Dict[Tuple[str, datetime.date], float]:
        """ Snaps the latest market from the rester service. If it cant find the rester service, returns the
            empty market.

        :returns: market w/ (flight id, flight date) as keys, flight prices as values.
        """

        if self._latest_market is None:
            return {}

        market_record = copy.deepcopy(self._latest_market)  # take the latest market
        try:  # decode this
            market_value = market_record.value
        except Exception as e:
            logger.error(f'Couldnt get the value from market: {e}.')
            return {}

        return self._decode_mkt(loads(market_value))

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

    def _handle_mkt_events(self) -> None:
        """ Handles market events: Updates the _latest_market,

        :returns: handles the market signal.
        """

        for msg in self.__mkt_listener:
            if msg.value is None:  # TODO: check this condition.
                continue

            self.new_mkt_event()
            self._latest_market = msg

    def _publish_results(self, publish_delay : float = 1.) -> None:
        """ Publishing the results to the results topic thread.

        :param publish_delay: interval between publishing.
        :returns: publishes the results to the __result_publisher, doesn't return anything.
        """

        while True:
            logger.debug(f'Publishing new market results')
            self.__results_publisher.send(topic  = self._results_topic
                                         , value = bytearray(str(dumps(self.curr_market)), 'ascii')
                                         , )
            sleep(publish_delay)

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

        publish_thread = Thread(target = self._publish_results, daemon=True)
        publish_thread.start()

        # curr_mkt_thread computes current market, new_mkt_thread is computing new market
        controller_threads.extend([position_thread, market_events_thread, publish_thread])

        return controller_threads
