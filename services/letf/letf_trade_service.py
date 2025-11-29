""" Trade (and potentially) market producer.
    Market service publishes on mkt_events topic, mkt event is the uuid4 described above.
"""

import os
import logging
from dotenv import load_dotenv
from logging import getLogger

logging.basicConfig(level=logging.INFO)

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

frequency_of_market = 0.1
market_thread = letf_market_producer.create_thread(sleep_between_publish=frequency_of_market)

frequency_of_trades = 1
trade_thread = letf_trade_producer.create_thread(
    sleep_between_publish=frequency_of_trades
)

# this just simulates markets
market_thread.start()
# trade_thread.start()
