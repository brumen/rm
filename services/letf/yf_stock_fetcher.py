import yfinance as yf
import pandas as pd
import json
import time
import sys
import six.moves
import logging
import threading
from numpy import random

if sys.version_info >= (3, 12, 0):
    sys.modules["kafka.vendor.six.moves"] = six.moves


from typing import List, Dict, Any, Optional, Literal, Union
from kafka import KafkaProducer
from pydantic import BaseModel


_logger = logging.getLogger(__name__)


# --- 1. SabrParamNames (Rust Enum) ---
# Represented by a Python Literal type for strict value checking
SabrParamNames = Literal["Alpha", "Beta", "Rho", "Nu"]


# --- 2. SabrParameters (Rust Struct) ---
class SabrParameters(BaseModel):
    # Rust's pub(crate) struct is a Python class
    stock: str
    maturity: str  # Corresponds to Rust's NaiveDate (serialized as "YYYY-MM-DD")
    param_name: SabrParamNames


# --- 3. LETFMarketTypes (Rust Enum) ---
# Rust enums serialize as tagged unions. Pydantic's Union (or DiscriminatedUnion in v2)
# is the closest representation.
class Stock(BaseModel):
    # Enum variant with a value: "Stock": "TSLA"
    Stock: str


class Option(BaseModel):
    # Enum variant with a value: "Option": "TSLA250315C300"
    Option: str


class Sabr(BaseModel):
    # Enum variant with a complex struct value: "Sabr": { ... SabrParameters ... }
    Sabr: SabrParameters


class MarketBreak(BaseModel):
    Break: None


# Union of all possible variants, representing the full LETFMarketTypes enum
LETFMarketTypes = Union[Stock, Option, Sabr, MarketBreak]


class MarketValueTuple(BaseModel):
    """
    Represents the Rust tuple (LETFMarketTypes, f64).
    The f64 value is the market price, volatility, or parameter value.
    """
    # The first element is the Key/Type identifier
    market_key: LETFMarketTypes
    # The second element is the floating-point value
    value: float

    # Note on serialization: While the Pydantic model has field names
    # (market_key, value), when serialized as a tuple, the output
    # will still be a JSON array [market_key_json, value_json].
    # Pydantic's default `model_dump_json` creates a JSON object (dict)
    # with the keys "market_key" and "value". If you need the pure JSON array
    # for strict adherence to Rust's tuple serialization, you must post-process.

    # Custom method to get the pure list/tuple structure
    def to_json_array(self):
        """Returns the structure as a Python list ready for simple JSON array serialization."""
        return [self.market_key.model_dump(by_alias=True), self.value]


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

        # this is the form of the message below.
        # {
        #     "timestamp" : "2025-12-11T15:10:06.462924",
        #  this comes from yfinance - in msg
        #     "prices" : {
        #         "id" : "GOOG",
        #         "price" : 313.03,
        #         "time" : "1765483805000",
        #         "exchange" : "NMS",
        #         "quote_type" : 8,
        #         "market_hours" : 1,
        #         "change_percent" : -2.4828665,
        #         "day_volume" : "16314316",
        #         "change" : -7.970001,
        #         "last_size" : "100",
        #         "price_hint" : "2"
        #     }
        # }
        # msg_w_timestamp = msg | {
        #     "timestamp": pd.Timestamp.now().isoformat(),
        # }
        # message = {
        #     'Stock': msg_w_timestamp,
        # }

        stock_message = MarketValueTuple(
            market_key=Stock(Stock=msg['id']),
            value=msg['price'],
        )
        # [
        #     {"Stock", msg['id']},
        #     msg['price'],
        # ]

        try:
            self.producer.send(self.topic, stock_message.to_json_array())
            _logger.info(f"Streamed to {self.topic}: {stock_message}")
        except Exception as e:
            _logger.error(
                f"Error streaming message to {self.topic}: {e}, {stock_message}"
            )

    def _break_point(self):
        break_msg = MarketValueTuple(
            market_key=MarketBreak(Break=None),
            value=0.,  # irrelevant
        )
        try:
            self.producer.send(self.topic, break_msg.to_json_array())
            _logger.info(f"Streamed to {self.topic}: {break_msg}")
        except Exception as e:
            _logger.error(
                f"Error streaming message to {self.topic}: {e}, {break_msg}"
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


class YFStockKafkaStreamerSim(YFStockKafkaStreamer):

    def stream_prices(self, interval: int = 60):

        ticker_val = {
            ticker: 100
            for ticker in self.tickers
        }

        while True:
            for ticker in self.tickers:
                ticker_val[ticker] += random.normal(loc=0., scale=1.)
                msg = {
                    'id': ticker,
                    'price': ticker_val[ticker],
                }
                self._process_message(msg)
                self._break_point()  # send the break msg.
            time.sleep(interval)


def _fetcher_example():
    fetcher = YFStockFetcher(["AAPL", "MSFT", "GOOG"])
    _logger.info("Current Prices:", fetcher.fetch_repeated())


def _streamer_example():
    streamer = YFStockKafkaStreamer(["AAPL", "MSFT", "GOOG"])
    streamer.stream_prices(interval=1)


def _streamer_example_sim():
    streamer = YFStockKafkaStreamerSim(["AAPL", "MSFT", "GOOG", "NVDA"])
    streamer.stream_prices(interval=1)


if __name__ == "__main__":
    _streamer_example_sim()
