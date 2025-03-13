import logging
import sys
import six.moves

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def main_letf(result_idx='PV'):
    rp = ResultPublisherLETF(
       server_port_topic=('localhost', 9092, 'air_options.ao.results'),
       metric=result_idx,
    )

    rp.start()


def main_ao(result_idx='PV'):

    server_port_topic = ('192.168.1.107', 9092, 'air_options.ao.results')

    if result_idx == 'PV':
        rp = ResultPublisherKafkaPV(
            server_port_topic=server_port_topic,
            metric='PV',  # result_idx,
        )

    elif result_idx == 'PV01':
        rp = ResultPublisherKafkaPV01(
            server_port_topic=server_port_topic,
            metric=result_idx,
        )
    else:
        raise RuntimeError('Metric has to be either PV or PV01')

    rp.start()


if __name__ == '__main__':
    if len(sys.argv) > 2:  # we have enought arguments
        metric = sys.argv[1]
    else:
        metric = 'PV'

    main_ao(result_idx=metric)
