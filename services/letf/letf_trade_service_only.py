""" Trade producer only. Only produces trades, not markets.
"""

import os
import logging
import sys
from dotenv import load_dotenv
from logging import getLogger

logging.basicConfig(level=logging.INFO)

from rm.services.letf.trade_service import LETFTradeProducer

_logger = getLogger(__name__)

# start the leveraged etf market producer
load_dotenv()
KAFKA_HOST = os.getenv('HOST')  # '192.168.1.107'
KAFKA_PORT = os.getenv('KAFKA_PORT')  # 9092
POSITIONS_TOPIC = os.getenv('POSITIONS_TOPIC')

_logger.info(
    f'Starting market and trade service on {KAFKA_HOST}:{KAFKA_PORT}, '
    f'position_topic: {POSITIONS_TOPIC}'
)

letf_trade_producer = LETFTradeProducer(
    stocks=['AAPL', 'NVDA', ],
    server_port_topic=(KAFKA_HOST, KAFKA_PORT, POSITIONS_TOPIC, ),
    trade_nb_start=int(sys.argv[1]),
)


frequency_of_trades = 1
trade_thread = letf_trade_producer.create_thread(
    sleep_between_publish=frequency_of_trades
)

# this just simulates markets
# market_thread.start()
trade_thread.start()
