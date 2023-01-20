""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log
"""

import datetime
import logging
import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')


from typing     import List, Dict, Tuple, Any, Union, Generator, Optional
from markupsafe import escape
from flask      import Flask, Response, request

from ao.trade  import create_session, AOTrade, DeltaDict, AirOptionFlights


logging.basicConfig(filename='/tmp/trade_pv_restr.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

pv_rester = Flask(__name__)
pv_rester.debug = True
pv_rester.use_debugger = True

# TODO: REMOVE THESE GLOBALS AS WELL
# global vars,
session = create_session()  # TODO: THIS SESSION HERE IS WRONG
mkt_date = datetime.date(2016, 7, 1)
market = None  # initial market  # TODO: TYPE THIS DO MAKE IT READABLE

# TODO: FIX THIS - REMOVE GLOBAL VARS
# cached computed trades, a naive implementation
_trade_pvs = {}
_trade_pv01s = {}


def extract_trade_ids(trades : str) -> List[int]:
    """ Gets the trade ids from the trade string.

    :param trades: comma separated list of trade ids.
    :result: list of trades ids in integer type.
    """

    if ',' not in trades:
        return [int(trades), ]  # only 1 trade

    return [int(x) for x in trades.split(',')]


def construct_ao_trades(trade_ids : List[int]) -> List[AOTrade]:
    """ Constructs the AOTrades for the list of trade ids.

    :param trade_ids: list of trade ids for which AOTrades are created.
    :returns: list of aotrades corresponding to the trade ids.
    """

    # TODO: WHAT TO DO W/ THIS SESSION. THIS SESSION SI NOT NEEDED PERHAPS
    session = create_session()

    return session.query(AOTrade).filter(AOTrade.position_id.in_(trade_ids)).all()  # TODO: CAN WE DO A GENERATOR HERE???


@pv_rester.route('/market_date')
def get_market_date():
    """ Getting the market date.
    """

    global mkt_date
    return Response(mkt_date.strftime("%Y%m%d"))


@pv_rester.route('/market_date/<new_mkt_date>', methods = ['POST',])
def update_market_date(new_mkt_date: str):
    """ Dealing w/ setting and unsetting the market date.

    :param new_mkt_date: new market date to set
    """

    global mkt_date
    # post request, change date, return the same date
    mkt_date = datetime.datetime.strptime(new_mkt_date, '%Y%m%d')  # 20230205  dates
    return Response(mkt_date.strftime("%Y%m%d"))


@pv_rester.route('/market/<new_market>', methods=['POST', ])
def update_market(new_market):
    """ Updates the market to the

    """
    global market
    market = new_market
    return Response(new_market)  # TODO: SOME CONVERSION HERE.


@pv_rester.route('/pv_mkt/<trade_id>')
def trade_pv_mkt(trade_id):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trades : List[AOTrade] = construct_ao_trades(extract_trade_ids(escape(trade_id)))

    if not trades:
        return str(0)

    result : Dict[int, float] = {}  # result pvs for every trade id

    for trade in trades:
        trade_pos_id = trade.position_id
        trade_pv = _compute_trade_from_mkt(
            mkt_date,
            trade,
            'c',
            market,
            {'default_price': 100,
             'nb_sim': 5000,
             }
        )

        result[trade_pos_id] = trade_pv

    return result


@pv_rester.route('/pv/<trade_id>')
def trade_pv(trade_id):
    """ Computes the value of the trade given the current market.

    """

    trades = construct_ao_trades(extract_trade_ids(escape(trade_id)))

    if not trades:
        return str(0)

    result = {}
    for trade in trades:
        trade_pos_id = trade.position_id
        trade_pv = trade.PV(mkt_date) if trade_pos_id not in _trade_pvs else _trade_pvs[trade_pos_id]
        _trade_pvs[trade_pos_id] = trade_pv
        result[trade_pos_id] = trade_pv

    return result


# trade with market
def _compute_trade_from_mkt( mkt_date        : datetime.date
                             , ao_trade        : AOTrade
                             , trade_direction : str
                             , market          : Optional[Dict[Tuple[str, datetime.date], float]]
                             , ao_params       : Dict[str, Any]
                             , ) -> Dict[str, float]:
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
    """

    default_price = ao_params['default_price'] # =   : float = 200.
    nb_sim        = ao_params['nb_sim']  #          : int = 500


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
        flight_id = flight.flight_id
        carrier   = flight.carrier
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

    aof = AirOptionFlights( mkt_date, flights, ao_trade.strike)

    pv = aof.PV(nb_sim=nb_sim)
    pv01 = aof.PV01(nb_sim=nb_sim)
    trade_id = ao_trade.position_id

    return { 'PV'  : DeltaDict({trade_id: pv}) if trade_direction == 'c' else DeltaDict({trade_id: - pv})
           , 'PV01': pv01 if trade_direction == 'c' else - pv01
           , }


def _extract_market(market):
    """ Extract market from the string.

    """

    return market.json()  # TODO: FIX THIS HERE!



@pv_rester.route('/pv/<trade_ids>/<market>')
def trade_pv_market(trade_ids, market):
    """ Computes the trade id PVs given the market

    :param trade_ids: string separating the trade ids.
    :param market:

    """

    trade_id = escape(trade_id)

    trades : List[AOTrade] = extract_trade_ids(trade_id)

    if not trades:
        return str(0)

    market_d : Dict[Tuple[str, datetime.date], float] = _extract_market(market)

    result = {}
    for trade in trades:  # trade is AOTrade
        trade_pos_id = trade.position_id
        trade_pv = trade.PV(mkt_date) if trade_pos_id not in _trade_pvs else _trade_pvs[trade_pos_id]
        _trade_pvs[trade_pos_id] = trade_pv
        result[trade_pos_id] = trade_pv

        pvs = _compute_trade_from_mkt(
                             mkt_date
                             , trade
                             , trade_direction
                             , market_d
                             , ao_params
                             , )

    return result


def _value_portfolio_remote( self
                             , trade_ids : Union[List[Tuple[int, str]], Generator[Tuple[int, str], None, None]]
                             , ) -> List[Dict[str, Any]]:
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


@pv_rester.route('/pv01/<trade_id>')
def trade_pv01(trade_id):
    """ Returns the PV01 of the trade.
        Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trade_id = escape(trade_id)

    trades = extract_trade_ids(trade_id)

    if not trades:
        return str(0)

    # trade 0
    trade_0_pos_id = trades[0].position_id
    trade_0 = trades[0].PV01(mkt_date) if trade_0_pos_id not in trade_pv01s else _trade_pv01s[trade_0_pos_id]
    _trade_pv01s[trade_0_pos_id] = trade_0
    result = trade_0

    # TO FIX HERE
    for trade in trades[1:]:
        trade_pos_id = trade.position_id
        trade_pv01 = trade.PV01(mkt_date) if trade_pos_id not in trade_pv01s else _trade_pv01s[trade_pos_id]
        _trade_pv01s[trade_pos_id] = trade_pv01
        result += trade_pv01

    return result

    # aggregated
    #return sum( [DeltaDict(trade.PV01(mkt_date) if trade.position_id not in trade_pv01s else trade_pv01s[trade.position_id])
    #             for trade in trades[1:]]
    #            , start = DeltaDict(trades[0].PV01(mkt_date) if trades[0].position_id not in trade_pv01s else trade_pv01s[trades[0].position_id]))

    # non-aggregated
    # return {trade.position_id: trade.PV01(mkt_date) for trade in trades}


# @classmethod
# def _compute_trade_on_the_fly(cls
#                               , mkt_date        : datetime.date
#                               , ao_trade        : AOTrade
#                               , trade_direction : str
#                               , ao_params       : Dict[str, Any]
#                               , ) -> Dict[str, float]:
#     """ Compute trades by fetching the market data on-the-fly, meaning at the time that the trade is computed.
#         _compute_trade_from_mkt uses the same market for all trades (when it can).

#     :param mkt_date: market date.
#     :param ao_trade: ao trade to be values.
#     :param trade_direction: direction of the trade, 'c' for long, 'd' for short.
#     :param ao_params: parameters related to valuation/risk of the trade
#     :returns: PV and PV01 of the trade to be computed.
#     """

#     aof = AirOptionFlights.from_flights( mkt_date, ao_trade.flights, ao_trade.strike)

#     nb_sim = ao_params['nb_sim']

#     pv = aof.PV(nb_sim=nb_sim)
#     pv01 = aof.PV01(nb_sim=nb_sim)
#     logger.debug(f'PV, PV01 of {ao_trade}: {pv, pv01}')

#     return { 'PV'  : DeltaDict({ao_trade.position_id: pv}) if trade_direction == 'c' else DeltaDict({ao_trade.position_id: - pv})
#            , 'PV01': pv01 if trade_direction == 'c' else - pv01
#            , }




# pv rester start
def main():
    pv_rester.run(port=5010)

# UNCOMMENT IF TO RUN RESTER.
main()
