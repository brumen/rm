import logging
import sys

sys.path.append('/home/brumen/work/')

from rm.result_publisher_by_trade import ( ResultPublisherRester
                                           , ResultPublisherKafka
                                           , ResultPublisherKafkaPV
                                           , ResultPublisherKafkaPV01
                                           , ResultPublisherKafkaPV_Useless
                                           , )

logging.basicConfig(filename = '/tmp/rm_results_by_trade.log', level = logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main(result_idx = 'PV'):
    if result_idx == 'PV01':
        rp = ResultPublisherKafkaPV01()
    else:
        rp = ResultPublisherKafkaPV_Useless()

    rp.start()


main(result_idx = sys.argv[1])
