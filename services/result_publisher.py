import logging
logging.basicConfig(filename = '/tmp/rm_results_by_trade.log', level = logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

import sys
sys.path.append('/home/brumen/work/')

from rm.result_publisher_by_trade import (
    ResultPublisherRester,
    ResultPublisherKafka,
    ResultPublisherKafkaPV,
    ResultPublisherKafkaPV01,
    ResultPublisherKafkaPV_Useless,
    ResultPublisherKafkaPV_Useless2,
    ResultPublisherKafkaPV01_Useless,
)


def main(result_idx = 'PV'):
    if result_idx == 'PV01':
        rp = ResultPublisherKafkaPV01_Useless()
    else:
        rp = ResultPublisherKafkaPV_Useless()

    rp.start()


main(result_idx = sys.argv[1])
