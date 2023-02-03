""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log
"""

import logging
import datetime
import logging
import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')


from typing import List, Dict, Tuple, Any, Union, Generator, Optional
from markupsafe import escape
from flask import Flask, Response, request
from json import dumps, loads

from pyspark import SparkContext, SparkConf
from functools import lru_cache

from ao.trade import AOTrade, DeltaDict, AirOptionFlights
from rm.market_service import AOMarketService
from rm.services.trade_api_pricers import (
    _compute_trade_from_mkt,
    _compute_trades_from_id,
    default_params,
    construct_ao_trades,
    extract_trade_ids,
    TradeDirection,
)


# logging
logging.basicConfig(filename='/tmp/trade_pv_restr.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# rester start
pv_rester = Flask(__name__)
pv_rester.debug = True
pv_rester.use_debugger = True


# GLOBAL VARIABLES.
# TODO: Check if these globals can be removed.
# market date
mkt_date = datetime.date(2016, 7, 1)
# market on which the trades are priced.
MARKET_TYPE = Optional[Dict[Tuple[str, datetime.date], float]]
ENCODED_MARKET_TYPE = Optional[Dict[str, float]]
market: MARKET_TYPE = {}


@pv_rester.route('/market_date', methods=['GET', 'POST', ])
def get_market_date() -> Response:
    """ Getting/setting the market date.
    """

    global mkt_date
    if request.method == 'GET':
        return Response(mkt_date.strftime("%Y%m%d"))

    # method is POST
    # post request, change date, return the same date
    new_mkt_date = request.form.get('market_date')
    if new_mkt_date is None:
        return Response(None)

    mkt_date = datetime.datetime.strptime(
        new_mkt_date, '%Y%m%d')  # 20230205  dates

    return Response(mkt_date.strftime("%Y%m%d"))


@pv_rester.route('/market', methods=['GET', 'POST',])
def get_market() -> Response:
    """ Returns the market type
    """

    global market
    if request.method == 'GET':  # get method
        return Response(dumps(AOMarketService.encode_from_tuple(market)))

    # post method
    new_market = loads(request.data).get('market')
    if new_market is None:
        return Response(None)

    decoded_new_mkt: Dict[Tuple[str, datetime.date],
                          float] = AOMarketService.decode_mkt_data(new_market)
    market = decoded_new_mkt  # update the market.

    return Response("Updated market")


@ pv_rester.route('/update_market', methods=['POST', ])
def update_market():
    """ Resets the market and updates it w/ the market provided.
    """

    market_to_update = request.form.get('market')
    global market
    decoded_update_market: Dict[Tuple[str, datetime.date],
                                float] = AOMarketService.decode_mkt_data(market_to_update)
    market |= decoded_update_market
    return Response(dumps(market))


@ pv_rester.route('/pv/<trade_id>')
def trade_pv(trade_id):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trades: List[AOTrade] = construct_ao_trades(
        extract_trade_ids(escape(trade_id)))

    if not trades:
        return str(0)

    result: Dict[int, float] = {}  # result pvs for every trade id

    for trade in trades:
        trade_pv: Dict[str, Any] = _compute_trade_from_mkt(
            mkt_date,
            trade,
            TradeDirection.LONG,
            market,
            default_params,  # TODO: A SERVICE FOR MANIPULATING PRICING PARAMS.
        )

        result |= trade_pv['PV']

    return result


@ lru_cache
def _set_spark_env() -> SparkContext:
    """ Creates the spark context.
    """

    spark_ctx = {'pyfile': r'/home/brumen/work/work_ao.zip', }

    spark_conf = SparkConf().setMaster('local[8]')

    sc = SparkContext.getOrCreate(spark_conf)
    if 'pyfile' in spark_ctx:
        sc.addPyFile(spark_ctx['pyfile'])

    return sc


def price_trades(trade_ids: List[int]) -> Dict[str, float]:
    """ Prices trades using the spark parallelization.

    :param trade_ids: trades that should be valued.
    """

    sc = _set_spark_env()

    return sc\
        .parallelize(trade_ids)\
        .map(value_trade)\
        .collect()

    # aggregated
    # return sum( [DeltaDict(trade.PV01(mkt_date) if trade.position_id not in trade_pv01s else trade_pv01s[trade.position_id])
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
