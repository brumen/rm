""" Rest point for pricing LETF trades.

"""
import logging
import redis
from typing import List, Dict
from flask import Flask, Response
from markupsafe import escape

from rm.services.trade_api import extract_trade_ids

logger = logging.getLogger(__name__)


letf_rester = Flask(__name__)
letf_rester.debug = True
letf_rester.use_debugger = True

redis_cli = redis.Redis(
    host='localhost',
    port=6379,
    decode_responses=True,
)


def get_from_redis(trade_ids: List[str]) -> List[Dict]:
    """ Get trade info from redis about these ids
    """

    return [redis_cli.get(trade_id) for trade_id in trade_ids]


@letf_rester.route('/trades/<trade_ids>', methods=['GET', ])
def get_trades(trade_ids) -> Response:
    """ Get information about trades specified in the request.
    """

    redis_trades = extract_trade_ids(escape(trade_ids))

    return get_from_redis(redis_trades)


@letf_rester.route('/price/<trade_ids>', methods=['GET', ])
def price_trades(trade_ids: List[str]) -> Response:
    """ Price trades on spark

    """
    letf_trades = get_from_redis(trade_ids)
    
