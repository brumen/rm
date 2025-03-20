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
import pandas as pd

from dotenv import load_dotenv
from typing import Dict, Tuple, Optional
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

        if not args:
            return Response(
                dumps(
                    {
                        market_name: AOMarketService.encode_from_tuple(market)
                        for market_name, market in ALL_MARKETS
                    }
                )
            )

        market_name = args.get('market')
        market: Optional[MARKET_TYPE] = ALL_MARKETS[market_name]

        if market is None:
            logger.warn(
                f'Market {market_name} does not exist. Responding None.'
            )
            return Response(None)

        logger.info(
            f'Market {market_name} exists - returning decoded market'
        )
        return Response(dumps(AOMarketService.encode_from_tuple(market)))

    # setting the newest market
    request_data = loads(request.data)
    new_market = request_data.get('market')
    market_name = request_data.get('market_type')
    logger.info(f'Setting market {market_name}.')

    if new_market is None:
        logger.warn(
            f'Setting {market_name} w/ None - Useless'
        )
        ALL_MARKETS.insert_preserve_names(
            {}, market_name=market_name,
        )
        return Response(f"New EMPTY market {market_name} added.")

    decoded_new_mkt: Dict[Tuple[str, datetime.date], float] = \
        AOMarketService.decode_mkt_data(new_market)

    logger.info(
        f'Setting market {market_name} w/ actual new market'
    )
    ALL_MARKETS.insert_preserve_names(
        decoded_new_mkt,
        market_name=market_name,
    )

    return Response("New market added.")


@pv_rester.route('/switch_market', methods=['POST', ])
def switch_market() -> Response:
    """ switch market name above to below, i.e.

    ALL_MARKETS[market_below] = ALL_MARKETS[market_above]
    """

    global ALL_MARKETS

    args = loads(request.data)

    market_name_below = args.get('market_below')
    market_name_above = args.get('market_above')

    logger.info(
        f'Switching markets {market_name_below} <- {market_name_above}'
    )

    market_above = ALL_MARKETS[market_name_above]

    if market_above is None:
        return Response(
            f'Could not find market {market_name_above}'
        )

    # all is set, switch markets
    ALL_MARKETS[market_name_below] = market_above

    return Response(
        f"Markets switched: {market_name_below} <- {market_name_above}"
    )


def market_to_pd(market: MARKET_TYPE, price_col_name='price') -> pd.DataFrame:
    """ Presents the market in a dataframe form. the price column is called price.

    :param market: market to be presented in tabular form.
    :param price_col_name: what should the price column be named.
    :returns: dataframe with the market in tabular form.
    """

    market_df = pd.DataFrame\
                  .from_dict(market, orient='index')\
                  .reset_index(names='airline_date')\
                  .rename(columns={0: price_col_name})\

    airlines = market_df['airline_date'].apply(lambda x: x[0])
    dep_dates = market_df['airline_date'].apply(lambda x: x[1])
    market_df['airline'] = airlines
    market_df['dep_date'] = dep_dates
    market_df = market_df.drop('airline_date', axis='columns')

    return market_df


@pv_rester.route('/market_display', methods=['GET',])
def market_display() -> Response:
    """ Displays the market in a proper table form.
    """

    global ALL_MARKETS

    args = request.args
    market_name = args.get('market')

    if market_name is None:
        # we concatenate all markets
        mkt_df = pd.DataFrame()
        for mkt_idx, (mn, mkt) in enumerate(ALL_MARKETS.items()):
            curr_mkt_df = market_to_pd(mkt, price_col_name=mn)  # mn = market name
            mkt_df = mkt_df.join(curr_mkt_df, how='outer', lsuffix=f'_{mn}')

        return Response(mkt_df.to_html())

    # displaying only a single market
    market: Optional[MARKET_TYPE] = ALL_MARKETS[market_name]

    if market is None:
        return Response(f"Unknown market name: {market_name}")

    market_df = market_to_pd(market)

    return Response(market_df.to_html())


# pv rester start
def main():
    load_dotenv()
    server_host = os.getenv('HOST')
    pv_rester.run(host=server_host, port=8000)  # IMPORTANT: NOTICE THE PORT


# IMPORTANT: this has to be called application, for mod_express
application = pv_rester
# UNCOMMENT IF TO RUN RESTER.
main()
