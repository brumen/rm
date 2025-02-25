""" The actual rester for the market service.
    The rester service is on: localhost:5000/mkt/get_market
    The market reported is a tuple, with two elements:
       1.st: uuid4 of the current market ("5342234234kasdasda-asd-asdasd-")
       2nd: dictionary where keys are flight_nb|departure_date,
              values are prices
              key = "UA06|20170608"; value=303
    Market service publishes on mkt_events topic, mkt event is
       the uuid4 described above.
"""

import logging
import sys
import six.moves

if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves


from rm.services.ao.market_service import AOMarketService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# starting the service
if __name__ == '__main__':
    aom = AOMarketService(time_interval=1)
    aom.run(sleep_delay=5, testing_shift=(1., 5.))
