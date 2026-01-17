""" Gets the data from the mkt_raw topic on Kafka and produces the
       sabr calibrating vals.
"""

import datetime
import logging
import sys
import six.moves
from typing import List, Optional
from pydantic import BaseModel

if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves

from rm.services.letf.yf_stock_fetcher import MarketValueTuple
from rm.services.letf.sabr_calibrator import SABRCalibratorMixin
from rm.services.letf.yf_option_chain_fetcher import YFOptionChainFetcher


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class YFSabrCalibrator(YFOptionChainFetcher):

    def __init__(
        self,
        tickers: List[str],
        kafka_bootstrap: Optional[str] = "192.168.1.107:9092",
        topic: str = "letf.mkt_raw",
    ):
        super().__init__(tickers=tickers, kafka_bootstrap=kafka_bootstrap, topic=topic)

        self.sabr_producer = self.producer
        self._sabr_calibrator = SABRCalibratorMixin()

    def _calibrate_chain(
        self,
        curr_date: datetime.date,
        ticker: str,
        expiry: datetime.date,
        option_chain: List,
        avg_price: float,
    ) -> MarketValueTuple:  # this is a generator
        curr_date = datetime.date.today()
        normalized_expiry = (expiry - curr_date).days / 252.0
        calibration = self._sabr_calibrator.calibrate(
            option_chain,
            avg_price,
            normalized_expiry,
        )
        expiry_str = expiry.strftime("%Y-%m-%d")
        # calibration looks like this:
        # {
        #     "alpha" : 0.25197037965404656,
        #     "beta" : 0.5,
        #     "rho" : 0.8916287692857908,
        #     "nu" : 0.0001,
        #     "success" : true,
        #     "message" : "CONVERGENCE: NORM_OF_PROJECTED_GRADIENT_<=_PGTOL",
        #     "ticker" : "MSFT",
        #     "expiry" : "2028-06-16",
        #     "F" : 92.59881578947369
        # }
        calibration |= {
            "ticker": ticker,
            "expiry": expiry_str,
            "F": avg_price,
        }
        # calibration_send will be decoded correctly by Rust.
        for param_name in ("Alpha", "Beta", "Rho", "Nu"):
            # calibration_send = [
            #     {
            #         'Sabr',
            #         {
            #             'SabrParameters',
            #             {
            #                 'stock': ticker,
            #                 'maturity': calibration['expiry'],
            #                 'param_name': param_name,
            #             },
            #         },
            #         calibration[param_name],
            #     }
            # ]
            sabr_params = SabrParameters(
                stock=ticker, maturity=expiry_str, param_name=param_name
            )
            sabr_instance = Sabr(Sabr=sabr_params)
            calibration_send = MarketValueTuple(
                market_key=sabr_instance,
                value=calibration[param_name],
            )

            yield calibration_send


def _calibrated_option_chain_example():
    fetcher = YFSabrCalibrator(
        [
            "AAPL",
            "MSFT",
            "GOOG",
        ]
    )
    fetcher.stream_option_chain()


if __name__ == "__main__":
    _calibrated_option_chain_example()
