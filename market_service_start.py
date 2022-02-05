""" The actual rester for the market service.
"""

import sys
import logging

from flask import Flask, jsonify

sys.path.append('/home/brumen/work/')

from rm.market_service import AOMarketService

logging.basicConfig(filename='/tmp/market_service_rester.log')
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

market_rester = Flask(__name__)
market_rester.debug = True
market_rester.use_debugger = True


# starting the service
aom = AOMarketService(time_interval=5)
aom.run()  # starts the publishing, non-blocking


@market_rester.route('/mkt/get_market')
def get_market():
    """ Returns the market & market id.
    """

    return jsonify(aom.encode_mkt())  # this encodes the latest market


market_rester.run()
