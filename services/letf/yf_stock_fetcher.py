import yfinance as yf
import pandas as pd
import json
import time
import sys
import six.moves
import logging
import threading


if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves


from typing import List, Dict, Any, Optional
from kafka import KafkaProducer


_logger = logging.getLogger(__name__)


class MarketStockFetcher:

    def __init__(
            self,
            tickers: List[str],
            kafka_bootstrap: Optional[str] = "192.168.1.107:9092",
            topic: str = "letf.mkt_raw",
    ):
        self.tickers = tickers
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        ) if kafka_bootstrap else None
        self.topic = topic


class YFStockFetcher(MarketStockFetcher):
    """
    Fetches current and historical stock prices using yfinance.
    """

    def fetch_current_prices(self) -> Dict[str, float]:
        """
        Fetches the latest stock prices for the given tickers.
        Returns a dictionary mapping ticker -> current price.
        """
        data = yf.download(self.tickers, period="1d", interval="1m", progress=False)
        prices = {}
        if isinstance(data.columns, pd.MultiIndex):
            for ticker in self.tickers:
                try:
                    prices[ticker] = float(data["Close"][ticker].dropna().iloc[-1])
                except Exception:
                    prices[ticker] = None
        else:
            prices[self.tickers[0]] = float(data["Close"].dropna().iloc[-1])
        return prices

    def fetch_historical_prices(
        self, period: str = "1mo", interval: str = "1d"
    ) -> Dict[str, Any]:
        """
        Fetches historical price data for the given tickers.
        Returns a dictionary mapping ticker -> DataFrame of OHLCV data.
        """
        data = {}
        for ticker in self.tickers:
            try:
                df = yf.download(
                    ticker, period=period, interval=interval, progress=False
                )
                data[ticker] = df
            except Exception as e:
                data[ticker] = {"error": str(e)}
        return data

    def fetch_repeated(self, interval: int = 60):
        """
        Streams stock prices to Kafka, polls periodically.
        """

        _logger.info("Fetching every {interval} seconds.")

        while True:
            prices = self.fetch_current_prices()
            message = {
                "timestamp": pd.Timestamp.now().isoformat(),
                "prices": prices,
            }
            self.producer.send(self.topic, message)
            _logger.info(f"Sent to {self.topic}: {message}")
            time.sleep(interval)


class YFStockKafkaStreamer(YFStockFetcher):
    """
    Streams fetched stock prices to a Kafka topic.
    """

    def _process_message(self, msg):
        """ processes the message got from yfinance.
        """

        message = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "prices": msg,
        }
        try:

            self.producer.send(self.topic, message)
            _logger.info(f"Streamed to {self.topic}: {message}")
        except Exception as e:
            _logger.error(
                f"Error streaming message to {self.topic}: {e}, {message}"
            )

    def stream_prices(self, interval: int = 60):
        """Streams stock prices to Kafka, uses yfinance's streaming (live) updates.
        """

        for ticker in self.tickers:
            t = threading.Thread(
                target=yf.Ticker(ticker).live,
                args=(self._process_message, )
            )
            t.daemon = True
            t.start()

        while True:
            time.sleep(1)


def _fetcher_example():
    fetcher = YFStockFetcher(["AAPL", "MSFT", "GOOG"])
    _logger.info("Current Prices:", fetcher.fetch_repeated())


def _streamer_example():
    streamer = YFStockKafkaStreamer(["AAPL", "MSFT", "GOOG"])
    streamer.stream_prices(interval=30)


# if __name__ == "__main__":
#     _streamer_example()
