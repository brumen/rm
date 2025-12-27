""" Gets the data from the mkt_raw topic on Kafka and produces the
       sabr calibrating vals.
"""
import yfinance as yf
import threading
import time
import datetime
import numpy as np
import json
import logging
from typing import Dict, Any, List
import sys
import six.moves

if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves

from typing import List, Dict, Any, Optional, Literal, Union
from kafka import KafkaProducer
from pydantic import BaseModel


from rm.services.letf.yf_stock_fetcher import (
    MarketStockFetcher,
    Option, Sabr, SabrParameters, MarketValueTuple,
)
from rm.services.letf.sabr_calibrator import SABRCalibratorMixin


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class YFSabrCalibrator:

    def __init__(
            self,
            tickers: List[str],
            kafka_bootstrap: Optional[str] = "192.168.1.107:9092",
            topic: str = "letf.mkt_raw",
    ):
        self.tickers = tickers
        self.sabr_producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        ) if kafka_bootstrap else None
        self.topic = topic
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
        normalized_expiry = (expiry - curr_date).days / 252.
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
            'ticker': ticker,
            'expiry': expiry_str,
            'F': avg_price,
        }
        # calibration_send will be decoded correctly by Rust.
        for param_name in ('Alpha', 'Beta', 'Rho', 'Nu'):
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
                stock=ticker,
                maturity=expiry_str,
                param_name=param_name
            )
            sabr_instance = Sabr(Sabr=sabr_params)
            calibration_send = MarketValueTuple(
                market_key=sabr_instance,
                value=calibration[param_name],
            )

            yield calibration_send
