""" The actual rester for the market service.
    The rester service is on: localhost:5000/mkt/get_market
    The market reported is a tuple, with two elements:
       1.st: uuid4 of the current market ("5342234234kasdasda-asd-asdasd-")
       2nd: dictionary where keys are flight_nb|departure_date, values are prices
            key = "UA06|20170608"; value=303
    Market service publishes on mkt_events topic, mkt event is the uuid4 described above.
"""

import sys
import logging
logger = logging.getLogger(__name__)

sys.path.append('/home/brumen/work/')

from rm.trade_service import LETFTradeProducer
from rm.services.letf_market_service import LETFMarketProducer


# start the leveraged etf market producer
letf_market_producer = LETFMarketProducer(['AAPL', 'NVDA', ])
letf_trade_producer = LETFTradeProducer(
    ['AAPL', 'NVDA', ],
    mkt_producer=letf_market_producer,
)

market_thread = letf_market_producer.create_thread(sleep_between_publish=10.)
trade_thread = letf_trade_producer.create_thread(sleep_between_publish=1.)

market_thread.start()
trade_thread.start()
