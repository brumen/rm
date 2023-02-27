""" The actual rester for the market service.
    The rester service is on: localhost:5000/mkt/get_market
    The market reported is a tuple, with two elements:
       1.st: uuid4 of the current market ("5342234234kasdasda-asd-asdasd-")
       2nd: dictionary where keys are flight_nb|departure_date, values are prices
            key = "UA06|20170608"; value=303
    Market service publishes on mkt_events topic, mkt event is the uuid4 described above.
"""

import logging
logging.basicConfig(filename='/tmp/market_service_rester.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import sys
sys.path.append('/home/brumen/work/')

from flask import Flask, jsonify
from rm.market_service import AOMarketService

market_rester = Flask(__name__)
market_rester.debug = True
market_rester.use_debugger = True


# starting the service
aom = AOMarketService(time_interval=5)
aom.run()  # starts the publishing, non-blocking


# @market_rester.route('/mkt/get_market')
# def get_market():
#     """ Returns the market & market id.
#     """
#
#     return jsonify(aom.encode_mkt())  # this encodes the latest market
#
#
# market_rester.run()
