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






# pv_rester.run(port=5010)
