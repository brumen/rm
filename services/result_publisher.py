import logging
import sys

sys.path.append('/home/brumen/work/')

from rm.result_publisher_by_trade import ResultPublisherRester, ResultPublisherKafka

logging.basicConfig(filename = '/tmp/rm_results_by_trade.log', level = logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    rp = ResultPublisherKafka()
    rp.start()


main()
