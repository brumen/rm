""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log

start proper server with:
    mod_wsgi-express start-server services/trade_api.py --processes 4 --port 5010
"""

import logging
# IMPORTANT: This logging config MUST BE HERE ON TOP, OTHERWISE IT DOES NOT WORK
logging.basicConfig(
    filename='/tmp/trade_pv_restr.log',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import datetime
import logging
import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')


from typing import List, Dict, Tuple, Any, Union, Generator, Optional
from markupsafe import escape
from flask import Flask, Response, request
from json import dumps, loads

from rm.services.trade_api_pricers import (
    _compute_trade_from_mkt,
    _compute_trades_from_id,
    default_params,
    construct_ao_trades,
    extract_trade_ids,
    TradeDirection,
    price_trades,
)


# rester start
pv_rester = Flask(__name__)
pv_rester.debug = True
pv_rester.use_debugger = True


# GLOBAL VARIABLES.
# TODO: Check if these globals can be removed.
# market date
mkt_date = datetime.date(2023, 3, 26)

# market on which the trades are priced.
MARKET_TYPE = Optional[Dict[str, float]]
market: MARKET_TYPE = {}  # current market
new_market : MARKET_TYPE = {}  # new market to price on.
future_market : MARKET_TYPE = {}  # future market, which will replace the new_market


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
        return Response(dumps(market))

    # post method
    new_market = loads(request.data)
    if new_market is None:
        return Response(None)

    decoded_new_mkt: Dict[str, float] = new_market
    market = decoded_new_mkt  # update the market.

    return Response("Updated CURRENT market.")


@pv_rester.route('/new_market', methods=['GET', 'POST',])
def get_new_market() -> Response:
    """ Storage for the new market.
    """

    global new_market
    if request.method == 'GET':  # get method
        return Response(dumps(AOMarketService.encode_from_tuple(new_market)))

    # post method
    replace_new_market = loads(request.data).get('market')
    if replace_new_market is None:
        return Response(None)

    decoded_replaced_new_mkt: Dict[Tuple[str, datetime.date],
                          float] = AOMarketService.decode_mkt_data(replace_new_market)
    new_market = decoded_replaced_new_mkt  # update the market.

    return Response("Updated NEW market.")


@pv_rester.route('/future_market', methods=['GET', 'POST',])
def get_future_market() -> Response:
    """ Storage for the future market. This market replaces the new market.
    """

    global future_market
    if request.method == 'GET':  # get method
        return Response(dumps(AOMarketService.encode_from_tuple(future_market)))

    # post method
    replace_future_market = loads(request.data).get('market')
    if replace_future_market is None:
        return Response(None)

    decoded_replaced_future_mkt: Dict[Tuple[str, datetime.date],
                          float] = AOMarketService.decode_mkt_data(replace_future_market)
    future_market = decoded_replaced_future_mkt  # update the market.

    return Response("Updated NEW market.")


@pv_rester.route('/switch_markets', methods=['GET',])
def switch_markets() -> Response:
    """ Switches the following markets:
        1. market <- new_market
        2. new_market <- future_market
    """

    global market, new_market, future_market, test_switch_markets

    market = new_market
    new_market = future_market

    return Response('Replaced current/new markets')


def trade_pv_market(
        trade_ids : List[int],
        market_ : MARKET_TYPE,
        metric : str = 'PV',
):
    """ Prices the trades with ids on the market provided.

    :param trade_ids: list of trade ids to price.
    :param market_: market for pricing.
    :param metric: metric to compute, either 'PV' or 'PV01'.
    """

    trades: List[AOTrade] = construct_ao_trades(trade_ids)

    if not trades:
        return {}

    result: Dict[int, float] = {}  # result pvs for every trade id

    for trade in trades:
        trade_pv: Dict[str, Any] = _compute_trade_from_mkt(
            mkt_date,
            trade,
            TradeDirection.LONG,
            market_,
            default_params,  # TODO: A SERVICE FOR MANIPULATING PRICING PARAMS.
        )

        result |= trade_pv[metric]

    return result


@ pv_rester.route('/pv/<trade_id>')
def trade_pv(trade_id):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, market)


@ pv_rester.route('/pv01/<trade_id>')
def trade_pv01(trade_id):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, market, 'PV01')


@ pv_rester.route('/pv_new/<trade_id>')
def trade_pv_new(trade_id):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, new_market)


@ pv_rester.route('/pv01_new/<trade_id>')
def trade_pv01_new(trade_id):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, new_market, 'PV01')


@ pv_rester.route('/pv_spark', methods=['POST',])
def trade_pv_spark() -> Response:
    """ Returns the PV of the trades presented.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    # requests has to have a form {"trades": "190,191"}
    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    trades : List[int]  = extract_trade_ids(escape(initial_trades))  # list of trade ids in the json encoded format

    if not trades:  # list is empty
        return Response(dump({}))

    return Response(dumps(price_trades(mkt_date, trades, 'c')))  # response of the priced trades


@ pv_rester.route('/pv01_spark', methods=['POST',])
def trade_pv01_spark() -> Response:
    """ Returns the PV of the trades presented.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    # requests has to have a form {"trades": "190,191"}
    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    trades : List[int]  = extract_trade_ids(escape(initial_trades))  # list of trade ids in the json encoded format

    if not trades:  # list is empty
        return Response(dump({}))

    return Response(dumps(price_trades(mkt_date, trades, 'c', 'PV01', )))  # response of the priced trades


@ pv_rester.route('/pv_spark_new', methods=['POST',])
def trade_pv_spark_new() -> Response:
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    trades: List[int] = extract_trade_ids(escape(initial_trades))

    if not trades:
        return Response(dumps({}))

    priced_trades = price_trades(mkt_date, trades, 'n')
    logger.info(f"PV01 {len(priced_trades.keys())} on NEW market using SPARK.")

    return Response(dumps(priced_trades))


@ pv_rester.route('/pv01_spark_new', methods=['POST',])
def trade_pv01_spark_new() -> Response:
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    trades: List[int] = extract_trade_ids(escape(initial_trades))

    if not trades:
        return Response(dumps({}))

    priced_trades = price_trades(mkt_date, trades, 'n', 'PV01',)
    logger.info(f"PV01 {len(priced_trades.keys())} on NEW market using SPARK.")

    return Response(dumps(priced_trades))


@pv_rester.route('/results', methods=['GET',])
def present_results():
    server = 'localhost'
    port = 9092
    topic = 'air_options.ao.results'

    # _subscriber = KafkaConsumer(topic, bootstrap_servers=f'{server}:{port}')
    _subscriber = list()

    for msg in _subscriber:
        logger.debug(f'Processing results from {server}@{port}@{topic}.')
        result_dict = loads(msg.value)  # value is json encoded

        # updating state
        yield result_dict



# pv rester start
def main():
    pv_rester.run(port=5010)


application = pv_rester  # IMPORTANT: this has to be called application, for mod_express
# UNCOMMENT IF TO RUN RESTER.
main()
