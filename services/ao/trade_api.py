""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log

start proper server with:
    mod_wsgi-express start-server services/trade_api.py
        --processes 4 --port 5010
"""

import logging
import datetime
from typing import List, Dict, Any, Tuple, Optional
from markupsafe import escape
from flask import Response, request, Flask
from json import dumps, loads

# IMPORTANT: This logging config MUST BE HERE ON TOP, OTHERWISE IT DOES NOT WORK
logging.basicConfig(
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')


from ao.trade import AOTrade, DeltaDict, AirOptionFlights
from rm.market_service import AOMarketService
from rm.services.ao.trade_api_pricers import (
    _compute_trade_from_mkt,
    _compute_trades_from_id,
    default_params,
    construct_ao_trades,
    extract_trade_ids,
    TradeDirection,
    price_trades,
    CurrNewMarket,
    PriceMetric,
    PRICING_SERVER_NAME,
)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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


ao_db = 'mysql://brumen@localhost/ao'
ao_engine = create_engine(ao_db)
ao_session = sessionmaker(bind=ao_engine)


def trade_pv_market(
        trade_ids: List[int],
        market_: MARKET_TYPE,
        metric: PriceMetric = PriceMetric.PV,
):
    """ Prices the trades with ids on the market provided.

    :param trade_ids: list of trade ids to price.
    :param market_: market for pricing.
    :param metric: metric to compute, either 'PV' or 'PV01'.
    """

    global ao_session

    with ao_session.begin() as session:
        trades: List[AOTrade] = construct_ao_trades(trade_ids, session)

        if not trades:
            return {}

        result: Dict[int, float] = {}  # result pvs for every trade id

        for trade in trades:
            trade_pv: Dict[str, Any] = _compute_trade_from_mkt(
                MKT_DATE,
                trade,
                metric,
                TradeDirection.LONG,
                market_,
                default_params,
            )

            result |= trade_pv

        return result


@pv_rester.route('/pv/<trade_id>')
def trade_pv(trade_id):
    """ Returns the PV of the trade.
           Trade can be either in the form of 200, or a list of trades,
           separated by e.g. 200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, MARKET)


@pv_rester.route('/pv01/<trade_id>')
def trade_pv01(trade_id):
    """ Returns the PV of the trade.
            Trade can be either in the form of 200, or a list of trades,
            separated by e.g. 200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, MARKET, PriceMetric.PV01)


@pv_rester.route('/pv/new/<trade_id>')
def trade_pv_new(trade_id):
    """ Returns the PV of the trade.
            Trade can be either in the form of 200, or a list of trades,
            separated by e.g. 200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, NEW_MARKET)


@pv_rester.route('/pv01/new/<trade_id>')
def trade_pv01_new(trade_id):
    """ Returns the PV of the trade.
            Trade can be either in the form of 200, or a list of trades,
            separated by e.g. 200, 201, 202
    """

    trade_ids = extract_trade_ids(escape(trade_id))

    return trade_pv_market(trade_ids, NEW_MARKET, PriceMetric.PV01)


# to test this:
# curl -X POST -F 'trades=189,190' localhost:8000/pv/spark

@pv_rester.route('/pv/spark', methods=['POST', ])
def trade_pv_spark() -> Response:
    """ Returns the PV of the trades presented.
            Trade can be either in the form of 200, or a list of trades,
            separated by e.g. 200, 201, 202
    """

    # requests has to have a form {"trades": "190,191"}
    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    # list of trade ids in the json encoded format
    trades: List[int] = extract_trade_ids(escape(initial_trades))

    if not trades:  # list is empty
        return Response(dumps({}))

    # response of the priced trades
    return Response(
        dumps(
            price_trades(
                MKT_DATE,
                trades,
                CurrNewMarket.CURRENT
            )
        )
    )


# to test this:
# curl -X POST -F 'trades=189,190' localhost:8000/pv/spark

@pv_rester.route('/pv01/spark', methods=['POST', ])
def trade_pv01_spark() -> Response:
    """ Returns the PV of the trades presented.
           Trade can be either in the form of 200, or a list of trades,
           separated by , e.g. 200, 201, 202
    """

    # requests has to have a form {"trades": "190,191"}
    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    # list of trade ids in the json encoded format
    trades: List[int] = extract_trade_ids(escape(initial_trades))

    if not trades:  # list is empty
        return Response(dumps({}))

    # response of the priced trades
    return Response(
        dumps(
            price_trades(
                MKT_DATE,
                trades,
                CurrNewMarket.CURRENT,
                PriceMetric.PV01,
            )
        )
    )


@pv_rester.route('/pv/spark_new', methods=['POST', ])
def trade_pv_spark_new() -> Response:
    """ Returns the PV of the trade.
            Trade can be either in the form of 200, or a list of trades,
            separated by , e.g. 200, 201, 202
    """

    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    trades: List[int] = extract_trade_ids(escape(initial_trades))

    if not trades:
        return Response(dumps({}))

    priced_trades = price_trades(MKT_DATE, trades, CurrNewMarket.NEW)
    logger.info(f"PV {len(priced_trades.keys())} on NEW market using SPARK.")

    return Response(dumps(priced_trades))


@pv_rester.route('/pv01/spark_new', methods=['POST', ])
def trade_pv01_spark_new() -> Response:
    """ Returns the PV of the trade.
            Trade can be either in the form of 200, or a list of trades,
            separated by e.g. 200, 201, 202
    """

    initial_trades = request.form.get('trades')

    if not initial_trades:
        return Response(dumps({}))

    trades: List[int] = extract_trade_ids(escape(initial_trades))

    if not trades:
        return Response(dumps({}))

    priced_trades = price_trades(
        MKT_DATE,
        trades,
        CurrNewMarket.NEW,
        PriceMetric.PV01,
    )
    logger.info(f"PV01 {len(priced_trades.keys())} on NEW market using SPARK.")

    return Response(dumps(priced_trades))


@pv_rester.route('/results', methods=['GET', ])
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


# pv rester start
def main():
    pv_rester.run(port=8000)


# IMPORTANT: this has to be called application, for mod_express
application = pv_rester
# UNCOMMENT IF TO RUN RESTER.
main()
