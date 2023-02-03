""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log
"""

import datetime
import logging
import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')


from enum import Enum
from typing import List, Dict, Tuple, Any, Union, Generator, Optional
from pyspark import SparkContext, SparkConf
from ao.trade import create_session, AOTrade, DeltaDict, AirOptionFlights


# logging
logging.basicConfig(filename='/tmp/trade_pv_restr.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# default params are the default pricing parameters, more to come.
default_params: Dict[str, Any] = {'default_price': 200., 'nb_sim': 500}


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


def construct_ao_trades(trade_ids: List[int]) -> List[AOTrade]:
    """ Constructs the AOTrades for the list of trade ids.

    :param trade_ids: list of trade ids for which AOTrades are created.
    :returns: list of aotrades corresponding to the trade ids.
    """

    # TODO: WHAT TO DO W/ THIS SESSION. THIS SESSION SI NOT NEEDED PERHAPS
    session = create_session()

    # TODO: CAN WE DO A GENERATOR HERE???
    return session.query(AOTrade).filter(AOTrade.position_id.in_(trade_ids)).all()


# trade with market
def _compute_trade_from_mkt(mkt_date: datetime.date, ao_trade: AOTrade, trade_direction: TradeDirection = TradeDirection.LONG, market: Optional[Dict[Tuple[str, datetime.date], float]] = None, ao_params: Optional[Dict[str, Any]] = None, ) -> Dict[str, DeltaDict]:
    """ Computes the trade from the market provided.

    :param mkt_date: market date
    :param ao_trade: ao trade to be values.
    :param trade_direction: direction of the trade, 'c' for long, 'd' for short
    :param market: market provided, a dictionary where keys are (flight_nb, flight_date), and values
            are flight prices for that flight.
    :param ao_params: parameters for the risk/valuation of the trade.
    :param default_price: default price if the flight could not be found in the market
    :param nb_sim: number of simulations used in pricing.
    :returns: PV and PV01 of the trade.
        PV is a dictionary of {'trade_id': float}, PV01 is the same type of dictionary
    """

    if ao_params is None:
        default_price = default_params['default_price']
        nb_sim = default_params['nb_sim']
    else:
        default_price = ao_params.get(
            'default_price', default_params.get('default_price'))
        nb_sim = ao_params.get('nb_sim', default_params.get('nb_sim'))

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

        dep_date = flight.dep_date.date()  # this is datetime.datetime by default
        flight_id = flight.flight_id
        carrier = flight.carrier
        flight_nb = f'{carrier}{flight_id}'
        logger.debug(f'Processing flight nb: {flight_nb}')

        if market is None:  # no idea about the market
            found_prices = flight.prices  # prices found in the database
            # find the last price, otherwise report a random price
            mkt_price = found_prices[-1].price if found_prices else default_price

        else:
            mkt_price = market.get((flight_nb, dep_date))
            if mkt_price is None:  # if market doesnt contain price
                found_prices = flight.prices  # prices found in the database
                # find the last price, otherwise report a random price
                mkt_price = found_prices[-1].price if found_prices else default_price

        flights.append((mkt_price, dep_date, flight_nb))

    aof = AirOptionFlights(mkt_date, flights, ao_trade.strike)

    pv = aof.PV(nb_sim=nb_sim)
    pv01 = aof.PV01(nb_sim=nb_sim)
    trade_id = ao_trade.position_id

    return {'PV': DeltaDict({trade_id: pv}) if trade_direction == TradeDirection.LONG else DeltaDict({trade_id: - pv}), 'PV01': pv01 if trade_direction == TradeDirection.LONG else - pv01, }


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
