""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log

start proper server with:
    mod_wsgi-express start-server services/trade_api.py
        --processes 4 --port 5010
"""

import os
import logging
import datetime
import six.moves
import sys

from dotenv import load_dotenv
from typing import List, Dict, Any, Tuple, Optional
from markupsafe import escape
from flask import Response, request, Flask
from json import dumps, loads
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# IMPORTANT: This logging config MUST BE HERE ON TOP, OTHERWISE IT DOES NOT WORK
logging.basicConfig(
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from ao.trade import AOTrade, DeltaDict, AirOptionFlights
from rm.services.ao.market_service import AOMarketService
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
from rm.market_tracker import MarketTracker


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
# future market, which will replace the new_market
ALL_MARKETS: MarketTracker = MarketTracker()  # list of all usable markets.

ao_db = 'mysql://brumen@localhost/ao'
ao_engine = create_engine(ao_db)
ao_session = sessionmaker(bind=ao_engine)


def price_trades_json(
        market_date: datetime.date,
        trade_ids: List[int],
        curr_new_mkt: CurrNewMarket,
        metric: PriceMetric = PriceMetric.PV,
):
    for trade_result in price_trades(
            market_date, trade_ids, curr_new_mkt, metric
    ):
        yield dumps(trade_result)


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


@pv_rester.route('/pricing')
def pricing():
    """ Get request for different markets, pricing metrics,
       and trade ids.
       Need to provide:
         market: Current or New
         metric: PV, PV01, PnL
         trade_ids: string like 200,201,202...
    """

    global ALL_MARKETS

    args = request.args
    market_name = args.get('market')

    _market = ALL_MARKETS[market_name]
    _metric = PriceMetric.from_string(args.get('metric'))
    _trade_ids = extract_trade_ids(escape(args.get('trade_ids')))

    return trade_pv_market(_trade_ids, _market, _metric)


# to test this:
# curl -X POST -F 'trades=189,190' localhost:8000/pv/spark
@pv_rester.route('/spark', methods=['POST', ])
def trade_pv_spark() -> Response:
    """ Returns the PV of the trades presented.
            Trade can be either in the form of 200, or a list of trades,
            separated by e.g. 200, 201, 202
    """

    # requests has to have a form {"trades": "190,191"}
    initial_trades = request.form.get('trades')
    metric = request.form.get('metric')  # PV or PV01
    market = request.form.get('market')  # Current or New

    if not initial_trades:
        return Response(dumps({}))

    # list of trade ids in the json encoded format
    trades: List[int] = extract_trade_ids(escape(initial_trades))

    if not trades:  # list is empty
        return Response(dumps({}))

    price_metric: PriceMetric = PriceMetric.from_string(metric)
    price_market: CurrNewMarket = CurrNewMarket.from_string(market)

    # response of the priced trades
    return Response(
        price_trades_json(
            MKT_DATE,
            trades,
            curr_new_mkt=price_market,
            metric=price_metric,
        )
    )


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
        new_mkt_date, '%Y%m%d'
    )  # 20230205  dates

    return Response(MKT_DATE.strftime("%Y%m%d"))


@pv_rester.route('/market', methods=['GET', 'POST', ])
def get_market() -> Response:
    """ Returns the market type
    """

    global ALL_MARKETS

    if request.method == 'GET':  # get method
        args = request.args

        market_name = args.get('market')
        market: Optional[MARKET_TYPE] = ALL_MARKETS[market_name]

        if market is None:
            return Response(None)

        return Response(dumps(AOMarketService.encode_from_tuple(market)))

    # setting the newest market
    request_data = loads(request.data)
    new_market = request_data.get('market')
    market_name = request_data.get('market_type')

    if new_market is None:
        return Response(None)

    decoded_new_mkt: Dict[Tuple[str, datetime.date], float] = \
        AOMarketService.decode_mkt_data(new_market)

    ALL_MARKETS.insert_preserve_names(decoded_new_mkt)

    return Response("New market added.")


@pv_rester.route('/switch_market', methods=['GET', ])
def switch_market() -> Response:
    """ switch market name above to below.
    """

    global ALL_MARKETS

    args = request.args

    market_name_below = args.get('market_below')
    market_name_above = args.get('market_above')

    market_above = ALL_MARKETS.get(market_name_above)
    if market_above is None:
        return Response(
            f'Could not find market {market_name_above}'
        )

    if market_name_below not in ALL_MARKETS:
        return Response(
            f'Could not find market {market_name_below}'
        )

    # all is set, switch markets
    ALL_MARKETS[market_name_below] = market_above

    return Response(
        f"Markets switched: {market_name_below} <- {market_name_above}"
    )


@pv_rester.route('/market_setup', methods=['POST', ])
def market_setup() -> Response:
    """ Sets up the number of markets involved.

        TODO: CURRENTLY THEY ARE ONLY SET TO ZERO
    """

    # currently set up 2 markets - Current and New
    #  initially they are empty markets
    ALL_MARKETS.new_market('Current', {})
    ALL_MARKETS.new_market('New', {})


# pv rester start
def main():
    load_dotenv()
    server_host = os.getenv('HOST')
    pv_rester.run(host=server_host, port=8000)


# IMPORTANT: this has to be called application, for mod_express
application = pv_rester
# UNCOMMENT IF TO RUN RESTER.
# main()
