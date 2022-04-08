""" market controller and rester.
    Market publishes on: localhost:5001/mkt/get_market_2
    writes logs to /tmp/market_service_start_2.log
"""

import logging
import sys

from flask     import Flask

sys.path.append('/home/brumen/work/')

logging.basicConfig(filename='/tmp/market_service_start_2.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from rm.controller_ao2 import ControllerAO

# Controller start
controller = ControllerAO()
controller.start()  # this is non-blocking

# market_rester_2 = Flask(__name__)
# market_rester_2.debug = True
# market_rester_2.use_debugger = True
#
#
# @market_rester_2.route('/mkt/get_market_2')
# def get_market():
#     """ Returns the market & market id.
#     """
#
#     return controller.encode_results()  # this encodes the latest results
#
#
# market_rester_2.run(port=5001)
