""" The actual rester for the market service.
    The rester service is on: localhost:5000/mkt/get_market
    The market reported is a tuple, with two elements:
       1.st: uuid4 of the current market ("5342234234kasdasda-asd-asdasd-")
       2nd: dictionary where keys are flight_nb|departure_date, values are prices
            key = "UA06|20170608"; value=303
    Market service publishes on mkt_events topic, mkt event is the uuid4 described above.
"""

import logging
logging.basicConfig(filename='/tmp/letf_trade_service.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import sys
sys.path.append('/home/brumen/work/')

from rm.trade_service import LETFTradeProducer
from rm.market_service import LETFProducer


# start the leveraged etf market producer
letf_market_producer = LETFProducer(['AAPL', 'NVDA', ])
letf_trade_producer = LETFTradeProducer(['AAPL', 'NVDA', ], market_producer=letf_market_producer)

market_thread = letf_market_producer.create_thread(sleep_between_publish=1.)
trade_thread = letf_trade_producer.create_thread(sleep_between_publish=6.)

market_thread.start()
trade_thread.start()
