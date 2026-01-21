""" Gets the data from the mkt_raw topic on Kafka and produces the
       sabr calibrating vals.
"""

import datetime
import logging
import sys
import six.moves
import numpy as np
import multiprocessing
from typing import List, Optional, Tuple
from pydantic import BaseModel

if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves

from rm.services.letf.yf_stock_fetcher import MarketValueTuple, Sabr, SabrParameters, Option
from mrds.mrds_calib import MrdsCalibMixin, MrdsModel
from rm.services.letf.yf_option_chain_fetcher import YFOptionChainFetcher


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class YFMrdsCalibrator(YFOptionChainFetcher):

    def __init__(
        self,
        tickers: List[str],
        kafka_bootstrap: Optional[str] = "192.168.1.107:9092",
        topic: str = "letf.mkt_raw",
    ):
        super().__init__(tickers=tickers, kafka_bootstrap=kafka_bootstrap, topic=topic)

        self.sabr_producer = self.producer
        mkt_date = datetime.date.today()
        self._mrds_calib = MrdsCalibMixin(mkt_date=mkt_date)

    @staticmethod
    def _extract_atm_val(chain_for_expiry: List, price: float) -> float:
        """Extract the ATM options. It attempts to match the strike as closely to stock price.

        :param chains_for_expiry:         
        :returns: volatility in % terms.
        """
        # calibrate to the closest 
        # elements of chain_for_expiry: 
        #   {'symbol': 'AAPL260116P00450000', 
        #    'lastTradeDate': '2025-12-29T16:07:20+00:00', 
        #    'volatility': 3.468751328125, 
        #    'strike': 450.0, 
        #    'ticker': 'AAPL', 
        #    'lastPrice': 176.65, 
        #    'expiry': datetime.date(2026, 1, 16), 
        #    'call_put': 'put'}

        # TODO: This should be improved -
        #   go through the list and select the entry w/ smallest deviation of strike from price
        closeness_to_strike = list(map(lambda x: np.abs(x['strike']-price), chain_for_expiry))
        min_closeness = min(closeness_to_strike)        
        closeness_idx = closeness_to_strike.index(min_closeness)

        return chain_for_expiry[closeness_idx]['volatility']

    def _parametrize_in_deltas(
        self,
        option_expiry: datetime.date,
        strike: float, 
        price: float, 
        atm_vol: float,
    ) -> List[Tuple[float, float]]:
        """
        parametrize the chain 
        for a particular expiry in terms of deltas, not prices.
        i.e. (delta, volatility)
        """

        ttm = self._mrds_calib._difference_to_market_date(option_expiry)
        return self._mrds_calib.strike_to_delta(strike, price, atm_vol, ttm)

    # def _calibrate_chain(
    #     self,
    #     curr_date: datetime.date,
    #     ticker: str,
    #     expiry: datetime.date,
    #     option_chain: List,
    # ) -> MarketValueTuple:  # this is a generator
    #     stock_price: float = self._get_ticker_price(ticker)
    #     normalized_expiry: float = (expiry - curr_date).days / 252.0
    #     # filter out the options for that expiry
    #     chain_for_expiry = filter(
    #         lambda x: x['epxiry'] == expiry,
    #         option_chain
    #     )
    #     atm_vol = self._extract_atm_val(chain_for_expiry, stock_price)
    #     # vol slice across deltas
    #     deltas_vols = self._parametrize_in_deltas(
    #         chain_for_expiry=chain_for_expiry,
    #         price=stock_price,
    #         atm_vol=atm_vol,
    #     )

    #     calibration = self._mrds_calib.calibrate(
    #         deltas_vols,
    #         stock_price,
    #         normalized_expiry,  # TODO: FINISH HERE!! 
    #     )

    #     # TODO: THIS PRODUCES RELEVANT PARAMETERS, WHICH SHOULD BE PUT ON BUS

    #     expiry_str = expiry.strftime("%Y-%m-%d")

    #     # calibration_send will be decoded correctly by Rust.
    #     for param_name in ("Alpha", "Beta", "Rho", "Nu"):
    #         # calibration_send = [
    #         #     {
    #         #         'Sabr',
    #         #         {
    #         #             'SabrParameters',
    #         #             {
    #         #                 'stock': ticker,
    #         #                 'maturity': calibration['expiry'],
    #         #                 'param_name': param_name,
    #         #             },
    #         #         },
    #         #         calibration[param_name],
    #         #     }
    #         # ]
    #         sabr_params = SabrParameters(
    #             stock=ticker, maturity=expiry_str, param_name=param_name
    #         )
    #         sabr_instance = Sabr(Sabr=sabr_params)
    #         calibration_send = MarketValueTuple(
    #             market_key=sabr_instance,
    #             value=calibration[param_name],
    #         )

    #         yield calibration_send

    def _calibrate_skew_onearg(self, expiry_option_chain):
        expiry, mrds_option_chain = expiry_option_chain
        _ = self._mrds_calib.calibrate_skew(expiry, mrds_option_chain)

    def _calibrate_skew_parallel(self, mrds_option_chain, parallel=False):
        
        if not parallel:
            for expiry, mrds_option_chain_for_expiry in mrds_option_chain.items():
                _logger.info(
                    f'Calibrating for maturity: {expiry}'
                )
                _ = self._mrds_calib.calibrate_skew(expiry, mrds_option_chain_for_expiry)
            return

        # calibration in parallel
        with multiprocessing.Pool() as pool:
            _ = pool.map(self._calibrate_skew_onearg, list(mrds_option_chain.items()))

    def _one_iteration(self, ticker: str):

        # chains indexed by expiry date.
        # all options have to be stored.
        stock_price = self._get_ticker_price(ticker)
        mrds_option_chain: Dict[datetime.date, Any] = {}
        atm_vols: Dict[datetime.date, float] = {}
        # remove the expiries on the same day
        expiries_for_ticker = [expiry for expiry in self._get_ticker_expiries(ticker) if expiry > datetime.date.today()]
        for expiry in expiries_for_ticker:
            option_chain_for_expiry = self.fetch_option_chain(ticker, expiry)
            atm_vols[expiry] = self._extract_atm_val(option_chain_for_expiry, stock_price)            
            mrds_option_chain[expiry] = []
            for option_entry in option_chain_for_expiry:
                option_entry_delta = self._parametrize_in_deltas(
                    expiry,
                    option_entry['strike'], 
                    stock_price, 
                    atm_vols[expiry]
                )
                # filtering the option deltas.
                call_put = option_entry['call_put']  # 'call' or 'put'
                call_cnd = (call_put == 'call') and (0.05 < option_entry_delta < 0.95)
                put_cnd = call_put == 'put' and (-0.95 < option_entry_delta < -0.05)
                if call_cnd or put_cnd:
                    mrds_option_chain[expiry].append(
                        (option_entry_delta, option_entry['volatility'])
                    )

            for option_entry in option_chain_for_expiry:
                # this just sends the original options.
                option_entry_publish = self._tsf_one_option(option_entry)
                self._one_entry_send(option_entry_publish)  # send just the option. 

        # calibrate the model to the parameters obtained.
        atm_vols_l = list(atm_vols.items())
        ksr = self._mrds_calib._kappa_sigma_rho(atm_vols_l)  # this sets kappa_sigma_rho
        k1 = ksr["kappa_1"]
        k2 = ksr["kappa_2"]
        s1 = ksr["sigma_1"]
        s2 = ksr["sigma_2"]
        rho = ksr["rho"]

        # setting betas
        for fwd_date, atm_vol in atm_vols_l:
            _ = self._mrds_calib.beta_T_calib(fwd_date, atm_vol, (k1, k2), (s1, s2), rho)
        
        # skew calibration, the most expensive.
        self._calibrate_skew_parallel(mrds_option_chain)

        # publish all of these things back onto the bus
        mrds_calib_model = MarketValueTuple(
            market_key = MrdsModel(
                ksr = ((k1, k2), (s1, s2), rho),
                expiries = expiries_for_ticker,
                skews = self._mrds_calib._c_vec,
                betas = self._mrds_calib._beta_T,
            ), 
            value=0,  # unimportant value.
        )

        # sending the MRDS calibrated model to 
        self._one_entry_send(mrds_calib_model)

def _calibrated_option_chain_example():
    fetcher = YFMrdsCalibrator(
        [
            "AAPL",
            # "MSFT",
            # "GOOG",
        ]
    )
    fetcher.stream_option_chain()


if __name__ == "__main__":
    _calibrated_option_chain_example()
