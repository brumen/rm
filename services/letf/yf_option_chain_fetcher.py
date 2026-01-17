import yfinance as yf
import threading
import time
import datetime
import numpy as np
import logging
from typing import Dict, Any, List

from rm.services.letf.yf_stock_fetcher import (
    MarketStockFetcher,
    Option,
    Sabr,
    SabrParameters,
    MarketValueTuple,
)
from rm.services.letf.sabr_calibrator import SABRCalibratorMixin


logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)


class YFOptionChainFetcher(MarketStockFetcher):
    """
    Fetches option chain data for selected stocks using yfinance.
    """

    def _get_ticker_price(self, ticker: str) -> float:
        """Gets the latest ticker price. """
        ticker = yf.Ticker(ticker)
        latest_data = ticker.history(period="1d")
        latest_price = latest_data['Close'].iloc[-1]
        return latest_price

    def _get_ticker_expiries(self, ticker: str) -> List[datetime.date]:
        try:
            stock = yf.Ticker(ticker)
            option_expiries = stock.options
            return [
                datetime.datetime.strptime(opt_expiry, "%Y-%m-%d").date()
                for opt_expiry in option_expiries
            ]
        except Exception as e:
            _logger.error(f"Error fetching option expiries: {e}")
            return []

    def fetch_option_chain(
        self, ticker: str, expiry: datetime.date
    ) -> List[Dict[str, Any]]:
        """
        Fetches the full option chain (calls and puts) for a given ticker.
        Returns a dictionary with 'calls' and 'puts' DataFrames.

        :param ticker: ticker for which options are fetched
        :param expiry: expiry for which options are fetched.
        """

        expiry_str = expiry.strftime("%Y-%m-%d")
        try:
            stock = yf.Ticker(ticker)
            chain = stock.option_chain(expiry_str)

        except Exception as e:
            _logger.error(f"Could not get option chain. Returning empty list: {e}")
            return []

        # construct call/put chain
        call_chain = chain.calls.to_records()
        put_chain = chain.puts.to_records()

        all_chains = []
        for call_opt in call_chain:
            call_opt_d = {
                "symbol": call_opt[1],
                "lastTradeDate": call_opt[2].isoformat(),
                "volatility": call_opt[-4],
                "strike": call_opt[3],
                "ticker": ticker,
                "lastPrice": call_opt[4],
                "expiry": expiry,
                "call_put": "call",
            }
            all_chains.append(call_opt_d)

        for call_opt in put_chain:
            call_opt_d = {
                "symbol": call_opt[1],
                "lastTradeDate": call_opt[2].isoformat(),
                "volatility": call_opt[-4],
                "strike": call_opt[3],
                "ticker": ticker,
                "lastPrice": call_opt[4],
                "expiry": expiry,
                "call_put": "put",
            }
            all_chains.append(call_opt_d)

        return all_chains

    @staticmethod
    def _tsf_one_option(option_entry: Dict[str, Any]) -> MarketValueTuple:
        option_entry_str = option_entry
        expiry_str = option_entry["expiry"].strftime("%Y-%m-%d")
        option_entry_str["expiry"] = expiry_str
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
            market_key=Option(Option=option_entry_str["symbol"]),
            value=option_entry_str["lastPrice"],
        )

        return option_send

    def chains_for_ticker_publish(self, ticker: str) -> MarketValueTuple:
        ticker_expiries: List[datetime.date] = self._get_ticker_expiries(ticker)
        for expiry in ticker_expiries:
            option_chain = self.fetch_option_chain(ticker, expiry)
            for option_entry in option_chain:
                option_to_send = self._tsf_one_option(option_entry)
                yield option_send

    def _one_entry_send(self, option_send):
        try:
            self.producer.send(self.topic, option_send.to_json_array())
            _logger.debug(f"Sent to {self.topic}: {option_send}")
        except Exception as e:
            _logger.error(f"Could not send to {self.topic}: {e}")

    def _one_iteration(self, ticker: str):
        """ Runs one iteration for the ticker.        
        """

        for option_send in self.chains_for_ticker_publish(ticker):
            self._one_entry_send(option_send)

    def publish_option_chains(self, timeout: int = 60):
        """
        Publishes all option entries for all chains,
           including all the 
        """

        while True:
            for ticker in self.tickers:
                self._one_iteration(ticker)
            time.sleep(timeout)

    def stream_option_chain(self):
        for ticker in self.tickers:
            t = threading.Thread(
                target=self.publish_option_chains,
            )
            t.daemon = True
            t.start()

        while True:
            time.sleep(5)


def _option_chain_example():
    fetcher = YFOptionChainFetcher(
        [
            "AAPL",
            "MSFT",
            "GOOG",
        ]
    )
    fetcher.stream_option_chain()


if __name__ == "__main__":
    _option_chain_example()
