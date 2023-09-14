import logging
import sys
logging.basicConfig(
    filename='/tmp/rm_results_by_trade.log',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

sys.path.append('/home/brumen/work/')

from rm.result_publisher_by_trade import (
    ResultPublisherRester,
    ResultPublisherKafka,
    ResultPublisherKafkaPV,
    ResultPublisherKafkaPV01,
    ResultPublisherKafkaPV_Useless,
    ResultPublisherKafkaPV_Useless2,
    ResultPublisherKafkaPV01_Useless,
    ResultPublisherLETF,
)


def main(result_idx='PV'):
    rp = ResultPublisherLETF(
        server_port_topic=('localhost', 9092, 'letf.risk'),
        metric=result_idx,
    )
    rp.start()


main(result_idx=sys.argv[1])
