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

from ao.trade import AOTrade, DeltaDict, AirOptionFlights
from rm.market_service import AOMarketService
from rm.services.trade_api_pricers import (
    _compute_trade_from_mkt,
    _compute_trades_from_id,
    default_params,
    construct_ao_trades,
    extract_trade_ids,
    TradeDirection,
    price_trades,
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


@ pv_rester.route('/pv_spark/<trade_ids>')
def trade_pv_spark(trade_ids):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trades: List[int] = extract_trade_ids(escape(trade_ids))

    if not trades:
        return str(0)

    global mkt_date
    return price_trades(mkt_date, trades)


# pv rester start
def main():
    pv_rester.run(port=5010)


# UNCOMMENT IF TO RUN RESTER.
main()
