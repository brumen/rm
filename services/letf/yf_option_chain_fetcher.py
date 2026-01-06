import yfinance as yf
import threading
import time
import datetime
import numpy as np
import logging
from typing import Dict, Any, List

from rm.services.letf.yf_stock_fetcher import (
    MarketStockFetcher,
    Option, Sabr, SabrParameters, MarketValueTuple,
)
from rm.services.letf.sabr_calibrator import SABRCalibratorMixin


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class YFOptionChainFetcher(MarketStockFetcher):
    """
    Fetches option chain data for selected stocks using yfinance.
    """

    def _get_ticker_expiries(self, ticker: str) -> List[datetime.date]:
        try:
            stock = yf.Ticker(ticker)
            option_expiries = stock.options
            return [
                datetime.datetime.strptime(opt_expiry, '%Y-%m-%d').date()
                for opt_expiry in option_expiries
            ]
        except Exception as e:
            _logger.error(f"Error fetching option expiries: {e}")
            return []

    def fetch_option_chain(self, ticker: str, expiry: datetime.date) -> List[Dict[str, Any]]:
        """
        Fetches the full option chain (calls and puts) for a given ticker.
        Returns a dictionary with 'calls' and 'puts' DataFrames.
        """

        expiry_str = expiry.strftime("%Y-%m-%d")
        try:
            stock = yf.Ticker(ticker)
            chain = stock.option_chain(expiry_str)

        except Exception as e:
            _logger.error(
                f"Could not get option chain. Returning empty list: {e}"
            )
            return []

        # construct call/put chain
        call_chain = chain.calls.to_records()
        put_chain = chain.puts.to_records()

        all_chains = []
        for call_opt in call_chain:
            call_opt_d = {
                'symbol': call_opt[1],
                'lastTradeDate': call_opt[2].isoformat(),
                'volatility': call_opt[-4],
                'strike': call_opt[3],
                'ticker': ticker,
                'lastPrice': call_opt[4],
                'expiry': expiry,
                'call_put': 'call',
            }
            all_chains.append(call_opt_d)

        for call_opt in put_chain:
            call_opt_d = {
                'symbol': call_opt[1],
                'lastTradeDate': call_opt[2].isoformat(),
                'volatility': call_opt[-4],
                'strike': call_opt[3],
                'ticker': ticker,
                'lastPrice': call_opt[4],
                'expiry': expiry,
                'call_put': 'put',
            }
            all_chains.append(call_opt_d)

        return all_chains

    def fetch_all_option_chains(self, expiry: datetime.date) -> Dict[str, Any]:
        """
        Fetches option chains for all tickers in the list.
        Returns a dictionary mapping ticker -> option chain data.
        """
        results = {}
        for ticker in self.tickers:
            results[ticker] = self.fetch_option_chain(ticker, expiry)
        return results

    def fetch_chains_for_ticker(self, ticker: str):
        ticker_expiries = self._get_ticker_expiries(ticker)
        for expiry in ticker_expiries:
            option_chain = self.fetch_option_chain(ticker, expiry)
            avg_price = np.mean([opt['lastPrice'] for opt in option_chain])
            for option_entry in option_chain:
                option_entry_str = option_entry
                expiry_str = option_entry['expiry'].strftime('%Y-%m-%d')
                option_entry_str['expiry'] = expiry_str
                # option_entry_str is this:
                # {
                #     "symbol" : "MSFT280616P00700000",
                #     "lastTradeDate" : "2025-12-05T15:29:27+00:00",
                #     "volatility" : 0.000010000000000000003,
                #     "strike" : 700.0,
                #     "ticker" : "MSFT",
                #     "lastPrice" : 218.55,
                #     "expiry" : "2028-06-16"
                # }
                option_send = MarketValueTuple(
                    market_key=Option(Option=option_entry_str['symbol']),
                    value=option_entry_str['lastPrice'],
                )
                # option_send = [
                #     {
                #         "Option", option_entry_str['symbol']
                #     },
                #     option_entry_str['lastPrice'],
                # ]
                yield (option_send, avg_price)

    def fetch_all_chains_all_maturs(self):
        """ Genearates all tickers and all maturities.

        """
        for ticker in self.tickers:
            for oc in self.fetch_chains_for_ticker(ticker):
                yield oc

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
        calibration = SABRCalibratorMixin().calibrate(
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

    def _run_option_chains(self, timeout: int = 60):
        """ Timeout for 60 seconds.
        """

        while True:
            for ticker in self.tickers:
                # for ticker, option_chain in option_chains.items():
                ticker_expiries = self._get_ticker_expiries(ticker)
                for expiry in ticker_expiries:
                    option_chain = self.fetch_option_chain(ticker, expiry)
                    avg_price = np.mean([opt['lastPrice'] for opt in option_chain])
                    for option_entry in option_chain:
                        option_entry_str = option_entry
                        expiry_str = option_entry['expiry'].strftime('%Y-%m-%d')
                        option_entry_str['expiry'] = expiry_str
                        try:
                            # option_entry_str is this:
                            # {
                            #     "symbol" : "MSFT280616P00700000",
                            #     "lastTradeDate" : "2025-12-05T15:29:27+00:00",
                            #     "volatility" : 0.000010000000000000003,
                            #     "strike" : 700.0,
                            #     "ticker" : "MSFT",
                            #     "lastPrice" : 218.55,
                            #     "expiry" : "2028-06-16"
                            # }
                            option_send = MarketValueTuple(
                                market_key=Option(Option=option_entry_str['symbol']),
                                value=option_entry_str['lastPrice'],
                            )
                            # option_send = [
                            #     {
                            #         "Option", option_entry_str['symbol']
                            #     },
                            #     option_entry_str['lastPrice'],
                            # ]
                            self.producer.send(self.topic, option_send.to_json_array())
                            _logger.info(f'Sent to {self.topic}: {option_send}')
                        except Exception as e:
                            _logger.error(
                                f'Could not send to {self.topic}: {e}'
                            )

                    curr_date = datetime.date.today()
                    for calibration_entry in self._calibrate_chain(
                            curr_date,
                            ticker,
                            expiry,
                            option_chain,
                            avg_price,
                    ):
                        try:
                            self.producer.send(
                                self.topic, calibration_entry.to_json_array()
                            )
                            _logger.info(
                                f'Sent to {self.topic}: {calibration_entry}'
                            )
                        except Exception as e:
                            _logger.error(
                                f'Could not send to {self.topic}: {e}'
                            )

            time.sleep(timeout)

    def stream_option_chain(self):
        for ticker in self.tickers:
            t = threading.Thread(
                target=self._run_option_chains,
            )
            t.daemon = True
            t.start()

        while True:
            time.sleep(5)


def _option_chain_example():
    fetcher = YFOptionChainFetcher(["AAPL", "MSFT", 'GOOG',])
    fetcher.stream_option_chain()


if __name__ == '__main__':
    _option_chain_example()
