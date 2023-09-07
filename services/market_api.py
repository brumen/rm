""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log

start proper server with:
    mod_wsgi-express start-server services/trade_api.py --processes 4 --port 5010
"""

import datetime
import sys
from logging import getLogger
from typing import List, Dict, Tuple, Any, Generator, Optional
from flask import Flask, Response, request
from json import dumps, loads

# logging.basicConfig(
#     filename='/tmp/market_restr.log',
#     level=logging.INFO
# )
logger = getLogger(__name__)

if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')


from rm.market_service import AOMarketService


# rester start
pv_rester = Flask(__name__)
pv_rester.debug = True
pv_rester.use_debugger = True


# GLOBAL VARIABLES.
# TODO: Check if these globals can be removed.
# market date
MKT_DATE = datetime.date(2016, 7, 1)
# market on which the trades are priced.
MARKET_TYPE = Optional[Dict[Tuple[str, datetime.date], float]]
ENCODED_MARKET_TYPE = Optional[Dict[str, float]]
MARKET: MARKET_TYPE = {}  # current market
NEW_MARKET: MARKET_TYPE = {}  # new market to price on.
# future market, which will replace the new_market
FUTURE_MARKET: MARKET_TYPE = {}


@pv_rester.route('/market_date', methods=['GET', 'POST', ])
def get_market_date() -> Response:
    """ Getting/setting the market date.
    """

    global MKT_DATE
    if request.method == 'GET':
        return Response(MKT_DATE.strftime("%Y%m%d"))

    # method is POST
    # post request, change date, return the same date
    new_mkt_date = request.form.get('market_date')
    if new_mkt_date is None:
        return Response(None)

    MKT_DATE = datetime.datetime.strptime(
        new_mkt_date, '%Y%m%d')  # 20230205  dates

    return Response(MKT_DATE.strftime("%Y%m%d"))


@pv_rester.route('/market', methods=['GET', 'POST', ])
def get_market() -> Response:
    """ Returns the market type
    """

    global MARKET
    if request.method == 'GET':  # get method
        return Response(dumps(AOMarketService.encode_from_tuple(MARKET)))

    # post method
    new_market = loads(request.data).get('market')
    if new_market is None:
        return Response(None)

    decoded_new_mkt: Dict[Tuple[str, datetime.date],
                          float] = AOMarketService.decode_mkt_data(new_market)
    MARKET = decoded_new_mkt  # update the market.

    return Response("Updated CURRENT market.")


@pv_rester.route('/new_market', methods=['GET', 'POST', ])
def get_new_market() -> Response:
    """ Storage for the new market.
    """

    global NEW_MARKET
    if request.method == 'GET':  # get method
        return Response(dumps(AOMarketService.encode_from_tuple(NEW_MARKET)))

    # post method
    replace_new_market = loads(request.data).get('market')
    if replace_new_market is None:
        return Response(None)

    decoded_replaced_new_mkt: Dict[Tuple[str, datetime.date], float] = \
        AOMarketService.decode_mkt_data(replace_new_market)
    NEW_MARKET = decoded_replaced_new_mkt  # update the market.

    return Response("Updated NEW market.")


@pv_rester.route('/future_market', methods=['GET', 'POST', ])
def get_future_market() -> Response:
    """ Storage for the future market. This market replaces the new market.
    """

    global FUTURE_MARKET
    if request.method == 'GET':
        return Response(
            dumps(AOMarketService.encode_from_tuple(FUTURE_MARKET))
        )

    # post method
    replace_future_market = loads(request.data).get('market')
    if replace_future_market is None:
        return Response(None)

    decoded_replaced_future_mkt: Dict[Tuple[str, datetime.date], float] = \
        AOMarketService.decode_mkt_data(replace_future_market)
    FUTURE_MARKET = decoded_replaced_future_mkt  # update the market.

    return Response("Updated NEW market.")


@pv_rester.route('/switch_markets', methods=['GET', ])
def switch_markets() -> Response:
    """ Switches the following markets:
        1. market <- new_market
        2. new_market <- future_market
    """

    global MARKET, NEW_MARKET, FUTURE_MARKET

    MARKET = NEW_MARKET
    NEW_MARKET = FUTURE_MARKET

    return Response('Replaced current/new markets')
