""" The actual rester for the market service.
    The rester service is on: localhost:5000/mkt/get_market
    The market reported is a tuple, with two elements:
       1.st: uuid4 of the current market ("5342234234kasdasda-asd-asdasd-")
       2nd: dictionary where keys are flight_nb|departure_date, values are prices
            key = "UA06|20170608"; value=303
    Market service publishes on mkt_events topic, mkt event is the uuid4 described above.
"""

from typing import List, Tuple, Dict
from numpy import random
from time import sleep

from rm.base_producer import BaseProducer


class LETFMarketProducer(BaseProducer):

    def __init__(
            self,
            stocks: List[str],
            server_port_topic: Tuple[str, str, str] = (
                'localhost',
                9092,
                'letf.mkt',
            ),
    ):
        super().__init__(server_port_topic)
        self._stocks: List[str] = stocks

        # intermediate state for stock values.
        # stocks are in the form of {stock_name: stock_value},
        # like {'APL': 150., 'NVA': 300.}
        self.curr_stocks: Dict[str, float] = {
            stock: 50. + stock_idx * 5
            for stock_idx, stock in enumerate(self._stocks)
        }  # initial values

    def _value_to_publish(self, sleep_between_publish=11.):
        """ Keeps generating new fictitious market for stocks.

        :param sleep_between_publish: sleep time between individual publishes
        """

        while True:

            yield self.curr_stocks

            for stock in self._stocks:
                # add some random value to the current stock values
                self.curr_stocks[stock] += random.normal(loc=0., scale=1.)

            sleep(sleep_between_publish)
