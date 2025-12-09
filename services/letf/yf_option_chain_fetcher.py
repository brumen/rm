import yfinance as yf
import threading
import time
import datetime
import numpy as np
import logging
from typing import Dict, Any, List

from rm.services.letf.yf_stock_fetcher import MarketStockFetcher
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
                            self.producer.send(self.topic, option_entry_str)
                            _logger.info(f'Sent to {self.topic}: {option_entry_str}')
                        except Exception as e:
                            _logger.error(
                                f'Could not send to {self.topic}: {e}'
                            )

                    # now run calibration and post that
                    curr_date = datetime.date.today()
                    normalized_expiry = (expiry - curr_date).days / 252.
                    calibration = SABRCalibratorMixin().calibrate(
                        option_chain,
                        avg_price,
                        normalized_expiry,
                    )
                    expiry_str = expiry.strftime("%Y-%m-%d")
                    calibration |= {
                        'ticker': ticker,
                        'expiry': expiry_str,
                        'F': avg_price,
                    }
                    try:
                        self.producer.send(self.topic, calibration)
                        _logger.info(f'Sent to {self.topic}: {calibration}')
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
    fetcher = YFOptionChainFetcher(["AAPL", "MSFT"])
    fetcher._run_option_chains()
