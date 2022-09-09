""" Rester service for trade PV.
    Market publishes on: localhost:5010/trade_pv
    writes logs to /tmp/trade_pv_restr.log
"""

import datetime
import logging
import sys
if '/home/brumen/work/' not in sys.path:
    sys.path.append('/home/brumen/work/')


from typing     import List
from markupsafe import escape
from flask      import Flask

from ao.trade  import create_session, AOTrade, DeltaDict

logging.basicConfig(filename='/tmp/trade_pv_restr.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

pv_rester = Flask(__name__)
pv_rester.debug = True
pv_rester.use_debugger = True

# TODO: REMOVE THESE GLOBALS AS WELL
# global vars,
session = create_session()
mkt_date = datetime.date(2016, 7, 1)


# TODO: FIX THIS - REMOVE GLOBAL VARS
# cached computed trades, a naive implementation
_trade_pvs = {}
_trade_pv01s = {}


def extract_trade_ids(trades : str) -> List[int]:

    if ',' not in trades:
        trade_ids = [int(trades)]

    else:
        trade_ids = [int(x) for x in trades.split(',')]

    return session.query(AOTrade).filter(AOTrade.position_id.in_(trade_ids)).all()


@pv_rester.route('/pv/<trade_id>')
def trade_pv(trade_id):
    """ Returns the PV of the trade.
    Trade can be either in the form of 200, or a list of trades, separated by , - e.g.
        200, 201, 202
    """

    trade_id = escape(trade_id)

    trades = extract_trade_ids(trade_id)

    if not trades:
        return str(0)

    result = {}
    for trade in trades:
        trade_pos_id = trade.position_id
        trade_pv = trade.PV(mkt_date) if trade_pos_id not in trade_pvs else _trade_pvs[trade_pos_id]
        _trade_pvs[trade_pos_id] = trade_pv
        result[trade_pos_id] = trade_pv

    return result


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


# pv rester start
def main():
    pv_rester.run(port=5010)

# UNCOMMENT IF TO RUN RESTER.
# main()
