"""
Spot crypto price connector with Kafka streaming support.

This module provides a small, dependency-light connector for spot crypto prices
using Coinbase public market data endpoints.

Features
--------
- fetch current spot prices for one or more products
- fetch best bid/ask where available
- continuously poll spot prices
- publish spot price updates to Kafka as JSON

Notes
-----
This module uses HTTP polling rather than a WebSocket feed to keep the
implementation simple and dependency-light. For many internal services,
polling every 1-5 seconds is sufficient.

Example product ids:
- BTC-USD
- ETH-USD
- SOL-USD

Kafka message schema
--------------------
Messages are published as JSON like:

    {
      "type": "SpotPrice",
      "data": {
        "product_id": "BTC-USD",
        "price": 65000.12,
        "bid": 64999.99,
        "ask": 65000.25,
        "source": "coinbase"
      }
    }
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional
from pydantic import BaseModel

import json
import time
import requests

try:
    from kafka import KafkaProducer  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    KafkaProducer = None  # type: ignore[assignment]


DEFAULT_HTTP_URL = "https://api.exchange.coinbase.com"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC_SPOTS = "letf.mkt_raw"  # crypto.spot.prices"


class SpotPriceError(RuntimeError):
    pass


@dataclass(frozen=True)
class SpotPrice:
    """
    Spot price update.
    """

    product_id: str
    price: float
    bid: Optional[float]
    ask: Optional[float]
    source: str = "coinbase"


def _require_kafka() -> None:
    if KafkaProducer is None:  # pragma: no cover
        raise SpotPriceError(
            "Kafka publishing requires the optional dependency `kafka-python` "
            "(pip install kafka-python)."
        )


def _json_serializer(v: Any) -> bytes:
    return json.dumps(v, separators=(",", ":"), sort_keys=True).encode("utf-8")


class CoinbaseSpotHTTP:
    """
    Minimal Coinbase public HTTP client for spot prices.
    """

    def __init__(
        self, base_url: str = DEFAULT_HTTP_URL, timeout_s: float = 10.0
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "spot-prices/1.0",
            }
        )

    def _get(self, path: str) -> Any:
        url = f"{self._base_url}{path}"
        r = self._session.get(url, timeout=self._timeout_s)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            raise SpotPriceError(
                f"HTTP error calling {url}: {e} - body={r.text}"
            ) from e
        try:
            return r.json()
        except ValueError as e:
            raise SpotPriceError(f"Non-JSON response from {url}: {r.text}") from e

    def get_product_ticker(self, product_id: str) -> Dict[str, Any]:
        """
        Returns the raw ticker payload for a product.

        Endpoint:
            GET /products/<product_id>/ticker
        """
        data = self._get(f"/products/{product_id}/ticker")
        if not isinstance(data, dict):
            raise SpotPriceError(
                f"Unexpected ticker response type for {product_id}: {type(data)}"
            )
        return data

    def get_spot_price(self, product_id: str) -> SpotPrice:
        """
        Fetch current spot ticker for a single product.
        """
        data = self.get_product_ticker(product_id)

        raw_price = data.get("price")
        raw_bid = data.get("bid")
        raw_ask = data.get("ask")

        try:
            price = float(raw_price)
        except (TypeError, ValueError) as e:
            raise SpotPriceError(
                f"Ticker for {product_id} missing/invalid price: {data}"
            ) from e

        bid: Optional[float]
        ask: Optional[float]

        try:
            bid = float(raw_bid) if raw_bid is not None else None
        except (TypeError, ValueError):
            bid = None

        try:
            ask = float(raw_ask) if raw_ask is not None else None
        except (TypeError, ValueError):
            ask = None

        return SpotPrice(
            product_id=product_id,
            price=price,
            bid=bid,
            ask=ask,
            source="coinbase",
        )

    def get_spot_prices(self, product_ids: Iterable[str]) -> Dict[str, SpotPrice]:
        """
        Fetch current spot prices for multiple product ids.
        """
        out: Dict[str, SpotPrice] = {}
        for product_id in product_ids:
            out[product_id] = self.get_spot_price(product_id)
        return out


class SpotKafkaPublisher:
    """
    Kafka publisher for spot prices.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
        topic: str = DEFAULT_KAFKA_TOPIC_SPOTS,
        client_id: str = "spot_prices",
        linger_ms: int = 50,
    ) -> None:
        _require_kafka()
        self._topic = topic
        self._producer = KafkaProducer(  # type: ignore[call-arg]
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            value_serializer=_json_serializer,
            linger_ms=linger_ms,
        )

    def publish_price(self, msg: SpotPrice, timeout_s: Optional[float] = None) -> None:
        coin = msg.product_id.replace("-USD", "")
        payload = [{"Stock": coin}, msg.price]
        fut = self._producer.send(self._topic, value=payload)
        if timeout_s is not None:
            fut.get(timeout=timeout_s)

    def flush(self, timeout_s: float = 10.0) -> None:
        self._producer.flush(timeout=timeout_s)

    def close(self, timeout_s: float = 10.0) -> None:
        try:
            self.flush(timeout_s=timeout_s)
        finally:
            self._producer.close(timeout=timeout_s)


def fetch_spot_prices(
    product_ids: Iterable[str],
    *,
    base_url: str = DEFAULT_HTTP_URL,
    timeout_s: float = 10.0,
) -> Dict[str, SpotPrice]:
    """
    One-shot helper to fetch spot prices for multiple products.
    """
    client = CoinbaseSpotHTTP(base_url=base_url, timeout_s=timeout_s)
    return client.get_spot_prices(product_ids)


def print_spot_prices(
    product_ids: Iterable[str],
    *,
    base_url: str = DEFAULT_HTTP_URL,
    timeout_s: float = 10.0,
) -> None:
    """
    Fetch and print current spot prices for the requested products.
    """
    prices = fetch_spot_prices(
        product_ids=product_ids,
        base_url=base_url,
        timeout_s=timeout_s,
    )
    for product_id in sorted(prices.keys()):
        px = prices[product_id]
        print(
            f"{px.product_id} price={px.price} "
            f"bid={px.bid if px.bid is not None else 'NA'} "
            f"ask={px.ask if px.ask is not None else 'NA'}"
        )


def stream_spot_prices_to_kafka(
    product_ids: Iterable[str],
    *,
    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_KAFKA_TOPIC_SPOTS,
    base_url: str = DEFAULT_HTTP_URL,
    timeout_s: float = 10.0,
    interval_s: float = 2.0,
    limit_updates: Optional[int] = None,
    only_on_change: bool = False,
    flush_interval_s: float = 1.0,
) -> None:
    """
    Poll spot prices and publish updates to Kafka.

    Parameters
    ----------
    product_ids:
        Iterable of product ids like ["BTC-USD", "ETH-USD"].
    bootstrap_servers:
        Kafka bootstrap servers.
    topic:
        Kafka topic for outgoing spot price messages.
    base_url:
        Coinbase HTTP base URL.
    timeout_s:
        HTTP request timeout and Kafka send ack timeout.
    interval_s:
        Delay between polling rounds.
    limit_updates:
        Stop after this many published updates. If None, runs forever.
    only_on_change:
        If True, only publish when price/bid/ask changed from the previous value.
    flush_interval_s:
        Flush Kafka producer periodically.
    """
    client = CoinbaseSpotHTTP(base_url=base_url, timeout_s=timeout_s)
    publisher = SpotKafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    prev_by_product: Dict[str, SpotPrice] = {}
    sent = 0
    last_flush = time.time()
    normalized_products: List[str] = [str(p) for p in product_ids]

    try:
        while True:
            for product_id in normalized_products:
                px = client.get_spot_price(product_id)

                should_publish = True
                if only_on_change:
                    prev = prev_by_product.get(product_id)
                    should_publish = prev != px

                if should_publish:
                    publisher.publish_price(px, timeout_s=timeout_s)
                    prev_by_product[product_id] = px
                    sent += 1

                    now = time.time()
                    if flush_interval_s > 0 and now - last_flush >= flush_interval_s:
                        publisher.flush(timeout_s=timeout_s)
                        last_flush = now

                    if limit_updates is not None and sent >= limit_updates:
                        return
            time.sleep(1.2)
            time.sleep(interval_s)
    finally:
        publisher.close(timeout_s=timeout_s)


def __main__():
    stream_spot_prices_to_kafka(
        bootstrap_servers="192.168.1.50:9092",
        product_ids=[
            "ETH-USD",
            "BTC-USD",
            "SEI-USD",
            "MORPHO-USD",
            "AAVE-USD",
            "SOL-USD",
            "HYPE-USD",
        ],
        # [
        #    "ETH-USD",
        #    "BTC-USD",
        # ],
    )


__main__()
