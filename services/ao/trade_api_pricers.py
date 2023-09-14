""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log
"""

import logging
import datetime
import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')

from json import loads
from enum import Enum
from requests import get as requests_get
from typing import List, Dict, Tuple, Any, Optional
from pyspark import SparkContext, SparkConf
from sqlalchemy.exc import OperationalError
from functools import lru_cache
from ao.trade import create_session, AOTrade, DeltaDict
from rm.market_service import AOMarketService


logger = logging.getLogger(__name__)


# default params are the default pricing parameters, more to come.
default_params: Dict[str, Any] = {'default_price': 200., 'nb_sim': 500}

# common_session = create_session()


# common session, let's see
# ao_db = 'mysql://brumen@localhost/ao'
# ao_engine = create_engine(ao_db)
# ao_session = sessionmaker(bind=ao_engine)


class TradeDirection(Enum):
    LONG = 'c'
    SHORT = 'd'


def extract_trade_ids(trades: str) -> List[int]:
    """ Gets the trade ids from the trade string.

    :param trades: comma separated list of trade ids, like 189,190
    :result: list of trades ids in integer type.
    """

    if ',' not in trades:
        return [int(trades), ]  # only 1 trade

    return [int(x) for x in trades.split(',')]


def construct_ao_trades(trade_ids: List[int], session) -> List[AOTrade]:
    """ Constructs the AOTrades for the list of trade ids.

    :param trade_ids: list of trade ids for which AOTrades are created.
    :returns: list of aotrades corresponding to the trade ids.
    """

    # TODO: WHAT TO DO W/ THIS SESSION. THIS SESSION SI NOT NEEDED PERHAPS
    # session = create_session()
    # global common_session
    # session = common_session

    # global ao_session

    # with ao_session.begin() as session:
    return session.query(AOTrade)\
                  .filter(AOTrade.position_id.in_(trade_ids))\
                  .all()


# trade with market
def _compute_trade_from_mkt(
    mkt_date: datetime.date,
    ao_trade: AOTrade,
    trade_direction: TradeDirection = TradeDirection.LONG,
    market: Optional[Dict[Tuple[str, datetime.date], float]] = None,
    ao_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, DeltaDict]:
    """ Computes the trade from the market provided.

    :param mkt_date: market date
    :param ao_trade: ao trade to be values.
    :param trade_direction: direction of the trade, 'c' for long, 'd' for short
    :param market: market provided, a dictionary where keys
            are (flight_nb, flight_date), and values
            are flight prices for that flight.
    :param ao_params: parameters for the risk/valuation of the trade.
    :param default_price: default price if the flight could not be found in
            the market
    :param nb_sim: number of simulations used in pricing.
    :returns: PV and PV01 of the trade.
        PV is a dictionary of {'trade_id': float}, PV01 is the same type
        of dictionary
    """

    aof = ao_trade.aof_market(mkt_date, market, ao_params)
    nb_sim = ao_params['nb_sim']

    pv = aof.PV(nb_sim=nb_sim)
    pv01 = aof.PV01(nb_sim=nb_sim)
    trade_id = ao_trade.position_id

    if trade_direction == TradeDirection.LONG:
        return {
            'PV': DeltaDict({trade_id: pv}),
            'PV01': DeltaDict({trade_id: pv01}),
        }

    return {
        'PV': DeltaDict({trade_id: - pv}),
        'PV01': DeltaDict({trade_id: - pv01}),
    }


def _compute_trades_from_id(
        mkt_date: datetime.date,
        trade_ids: List[int],
        trade_direction: TradeDirection = TradeDirection.LONG,
        market: Optional[Dict[Tuple[str, datetime.date], float]] = None,
        ao_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, DeltaDict]:
    """ Computes the PV and PV01 of the trade with given id.

    """

    trades: List[AOTrade] = construct_ao_trades(trade_ids)

    if not trades:  # empty list
        return {'PV': {}, 'PV01': {}, }

    return _compute_trade_from_mkt(
        mkt_date,
        trades[0],
        trade_direction=trade_direction,
        market=market,
        ao_params=ao_params,
    )


@ lru_cache
def _set_spark_env() -> SparkContext:
    """ Creates the spark context.
    """

    spark_ctx = {'pyfile':
                 [
                     r'/home/brumen/work/work_ao.zip',
                     r'/home/brumen/work/work_rm.zip',
                 ]}

    spark_conf = SparkConf().setMaster('local[8]')

    sc = SparkContext.getOrCreate(spark_conf)
    if 'pyfile' in spark_ctx:
        for spark_pyfile in spark_ctx['pyfile']:
            sc.addPyFile(spark_pyfile)

    return sc


def _value_trade_spark(
    market_date_trade_id: Tuple[datetime.date, int]
):
    """ Values the trades """

    market_date, trade_id, curr_new_mkt = market_date_trade_id

    session = create_session()
    try:
        trade = session.query(AOTrade)\
                       .filter(AOTrade.position_id.in_([trade_id, ]))\
                       .all()

    except OperationalError as e:
        logger.warn(f"Could not obtain {trade_id} correctly from DB: {e}")
        return {}  # TODO: WRONG THIS IS WRONG

    #    trade = construct_ao_trades([trade_id,])
    if not trade:
        return {}

    # call the service for the market
    if curr_new_mkt == 'c':
        market = requests_get('http://localhost:5010/market')
    else:
        market = requests_get('http://localhost:5010/new_market')
    market_decoded = AOMarketService.decode_mkt_data(loads(market.content))

    return _compute_trade_from_mkt(
        market_date,
        trade[0],
        trade_direction=TradeDirection.LONG,
        market=market_decoded,
        ao_params=default_params,
        session=session,
    ).get('PV', {})  # TODO: THIS SHOUDLD BE FIXED.


# TODO: FIX THE RETURN ARGUMENTS OF THIS FUNCTION - THIS ONLY WORKS FOR PV.
def _price_explicit_trade(
        trade_mkt_date_mkt_id: Tuple[AOTrade,datetime.date, chr, str]
) -> Dict[str, float]:
    """ Function to be sent to spark to price a trade.
    """

    # metric = 'PV', 'PV01', ...
    market_date, trade, curr_new_mkt, metric = trade_mkt_date_mkt_id

    if not trade:
        return {}

    # call the service for the market
    if curr_new_mkt == 'c':
        market = requests_get('http://localhost:5010/market')
    else:
        market = requests_get('http://localhost:5010/new_market')
    market_decoded = AOMarketService.decode_mkt_data(loads(market.content))

    return _compute_trade_from_mkt(
        market_date,
        trade,
        trade_direction = TradeDirection.LONG,
        market = market_decoded,
        ao_params = default_params,
    ).get(metric, {})  # TODO: THIS SHOUDLD BE FIXED.


def price_trades(
        market_date: datetime.date,
        trade_ids: List[int],
        curr_new_mkt: chr,
        metric: str = 'PV',
) -> Dict[str, float]:
    """ Prices trades using the spark parallelization.

    :param trade_ids: trades that should be valued.
    :param curr_new_mkt: 'c' for current market, 'n' for new market
    :param metric: metric to compute, either 'PV', or 'PV01'.
    """

    sc = _set_spark_env()

    nb_trades = len(trade_ids)

    session = create_session()
    try:
        trades: List[AOTrade] = \
            session.query(AOTrade)\
                   .filter(AOTrade.position_id.in_(trade_ids))\
                   .all()

    except OperationalError as e:
        logger.warn(f"Could not obtain trades correctly from DB: {e}")
        logger.debug(f"Trades requested: {trade_ids}")
        return {}  # TODO: WRONG THIS IS WRONG

    for t in trades:
        t._aof(market_date)  # IMPORTANT: touching the trade. IMPORTANT

    trade_vals = sc\
        .parallelize(
            zip([market_date] * nb_trades,
                trades,
                [curr_new_mkt, ] * nb_trades,
                [metric, ] * nb_trades
                ))\
        .map(_price_explicit_trade)\
        .collect()  # TODO: YOU CAN REDUCE THIS ON SPARK AS WELL

    result_pv = {}
    for result_trade in trade_vals:
        result_pv |= result_trade

    return result_pv
