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
KAFKA_HOST = os.getenv("HOST")  # '192.168.1.107'
KAFKA_PORT = os.getenv("KAFKA_PORT")  # 9092
POSITIONS_TOPIC = os.getenv("POSITIONS_TOPIC")

_logger.info(
    f"Starting market and trade service on {KAFKA_HOST}:{KAFKA_PORT}, "
    f"position_topic: {POSITIONS_TOPIC}"
)

try:
    trade_nb_start = int(sys.argv[1])
except Exception as e:
    _logger.info(f"Using trade nb start: 1000 ({e})")
    trade_nb_start = 1000

letf_trade_producer = LETFTradeProducer(
    stocks=["AAPL", "NVDA", "GOOG", "MSFT"],
    server_port_topic=(
        KAFKA_HOST,
        KAFKA_PORT,
        POSITIONS_TOPIC,
    ),
    trade_nb_start=trade_nb_start,
)

try:
    frequency_of_trades = sys.argv[2]
except Exception as e:
    _logger.info(f"Frequency: 1 ({e})")
    frequency_of_trades = 1


# 1st arg: trade st. nb.
# 2nd arg. freq of trades.
if __name__ == "__main__":
    trade_thread = letf_trade_producer.create_thread(
        sleep_between_publish=frequency_of_trades
    )
    # inserts trades every 1 second
    trade_thread.start()
