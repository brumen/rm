""" The actual rester for the market service.
    The rester service is on: localhost:5000/mkt/get_market
    The market reported is a tuple, with two elements:
       1.st: uuid4 of the current market ("5342234234kasdasda-asd-asdasd-")
       2nd: dictionary where keys are flight_nb|departure_date, values are prices
            key = "UA06|20170608"; value=303
    Market service publishes on mkt_events topic, mkt event is the uuid4 described above.
"""

import os
from dotenv import load_dotenv
from logging import getLogger

from rm.services.letf.trade_service import LETFTradeProducer
from rm.services.letf.letf_market_service import LETFMarketProducer

_logger = getLogger(__name__)

# start the leveraged etf market producer
load_dotenv()
KAFKA_HOST = os.getenv('HOST')  # '192.168.1.107'
KAFKA_PORT = os.getenv('KAFKA_PORT')  # 9092
MKT_TOPIC = os.getenv('MKT_TOPIC')
POSITIONS_TOPIC = os.getenv('POSITIONS_TOPIC')

_logger.info(
    f'Starting market and trade service on {KAFKA_HOST}:{KAFKA_PORT}, '
    f'mkt topic: {MKT_TOPIC}, position_topic: {POSITIONS_TOPIC}'
)

letf_market_producer = LETFMarketProducer(
    stocks=['AAPL', 'NVDA', ],
    server_port_topic=(KAFKA_HOST, KAFKA_PORT, MKT_TOPIC),
)
letf_trade_producer = LETFTradeProducer(
    stocks=['AAPL', 'NVDA', ],
    server_port_topic=(KAFKA_HOST, KAFKA_PORT, POSITIONS_TOPIC, ),
    mkt_producer=letf_market_producer,
)

market_thread = letf_market_producer.create_thread(sleep_between_publish=3)
trade_thread = letf_trade_producer.create_thread(sleep_between_publish=3)

market_thread.start()
trade_thread.start()
