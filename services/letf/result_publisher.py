import logging
import sys

logging.basicConfig(
    # filename="/tmp/rm_results_by_trade.log",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

import six.moves

sys.modules["kafka.vendor.six.moves"] = six.moves
sys.path.append("/home/brumen/work/")

from rm.result_publisher_by_trade import (
    # ResultPublisherRester,
    # ResultPublisherKafka,
    # ResultPublisherKafkaPV,
    # ResultPublisherKafkaPV01,
    # ResultPublisherKafkaPV_Useless,
    # ResultPublisherKafkaPV_Useless2,
    # ResultPublisherKafkaPV01_Useless,
    ResultPublisherLETF,
)


def main(result_idx="PV", host="localhost", topic="letf.risk"):
    rp = ResultPublisherLETF(
        server_port_topic=(host, 9092, topic),
        metric=result_idx,
    )
    rp.start()


# run as python result_publisher.py PV 192.168.1.107
try:
    result_idx = sys.argv[1]
except Exception as e:
    result_idx = "PV"
    logger.info(f"Computing PV ({e})")

try:
    host = sys.argv[2]
except Exception as e:
    host = "192.168.1.50"
    logger.info(f"Host: 192.168.1.50 ({e})")

try:
    topic = sys.argv[3]
except Exception as e:
    topic = "letf.risk"
    logger.info(f"Topic: {e}")


main(result_idx=result_idx, host=host, topic=topic)
