import logging
import sys

sys.path.append('/home/brumen/work/')

from rm.result_publisher_by_trade import ResultPublisherRester

logging.basicConfig(filename = '/tmp/rm_results_by_trade.log', level = logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    rp = ResultPublisherRester()
    rp.start()


main()
