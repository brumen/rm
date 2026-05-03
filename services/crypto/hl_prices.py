"""
Hyperliquid price connector.

This module provides a small, dependency-light connector for Hyperliquid's
public APIs to fetch:
- mid prices (all markets or single market)
- best bid/ask via L2 snapshot

Notes
-----
Hyperliquid has both HTTP and WebSocket endpoints. For "prices" we keep it
simple and use HTTP, which is typically enough for polling or periodic fetches.

If you need true streaming prices, we can add a WebSocket mid-price stream,
but Hyperliquid's most commonly used stream is trades and L2 book updates.

References (public):
- https://hyperliquid.gitbook.io/hyperliquid-docs (API docs)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterator, Optional, Tuple

import json
import time

import requests

try:
    from kafka import KafkaProducer  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    KafkaProducer = None  # type: ignore[assignment]

try:
    # WebSocket support is optional to keep this connector dependency-light.
    # If installed, we can provide streaming prices via WS.
    import websocket  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    websocket = None  # type: ignore[assignment]


DEFAULT_HTTP_URL = "https://api.hyperliquid.xyz"
DEFAULT_WS_URL = "wss://api.hyperliquid.xyz/ws"

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC_MIDS = "letf.mkt_raw"  # "crypto.hl.mids"


class HyperliquidError(RuntimeError):
    pass


@dataclass(frozen=True)
class HLWsMidPrice:
    """
    Mid-price update from the WebSocket stream.
    """

    coin: str
    mid: float


def _require_kafka() -> None:
    if KafkaProducer is None:  # pragma: no cover
        raise HyperliquidError(
            "Kafka publishing requires the optional dependency `kafka-python` "
            "(pip install kafka-python)."
        )


def _json_serializer(v: Any) -> bytes:
    return json.dumps(v, separators=(",", ":"), sort_keys=True).encode("utf-8")


class HyperliquidHTTP:
    """
    Minimal Hyperliquid HTTP client.

    Uses requests with a persistent Session.
    """

    def __init__(
        self, base_url: str = DEFAULT_HTTP_URL, timeout_s: float = 10.0
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._session = requests.Session()

    def _post(self, path: str, payload: Dict[str, Any]) -> Any:
        url = f"{self._base_url}{path}"
        r = self._session.post(url, json=payload, timeout=self._timeout_s)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            raise HyperliquidError(
                f"HTTP error calling {url}: {e} - body={r.text}"
            ) from e
        try:
            return r.json()
        except ValueError as e:
            raise HyperliquidError(f"Non-JSON response from {url}: {r.text}") from e

    def get_all_mids(self) -> Dict[str, float]:
        """
        Returns mapping coin -> mid price.

        Endpoint: POST /info { "type": "allMids" }

        The response is typically a dict of string->string numbers; we coerce to float.
        """
        data = self._post("/info", {"type": "allMids"})
        if not isinstance(data, dict):
            raise HyperliquidError(f"Unexpected allMids response type: {type(data)}")

        mids: Dict[str, float] = {}
        for k, v in data.items():
            try:
                mids[str(k)] = float(v)
            except (TypeError, ValueError):
                # Skip malformed entries but keep the connector resilient.
                continue
        return mids

    def get_mid(self, coin: str) -> Optional[float]:
        """
        Convenience wrapper around `get_all_mids()` for a single coin.
        """
        return self.get_all_mids().get(coin)

    def get_l2_snapshot(self, coin: str) -> Dict[str, Any]:
        """
        Returns the L2 order book snapshot (raw JSON).

        Endpoint: POST /info { "type": "l2Book", "coin": "<coin>" }
        """
        data = self._post("/info", {"type": "l2Book", "coin": coin})
        if not isinstance(data, dict):
            raise HyperliquidError(f"Unexpected l2Book response type: {type(data)}")
        return data

    def get_best_bid_ask(self, coin: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Returns (best_bid, best_ask) extracted from the L2 snapshot.
        """
        snap = self.get_l2_snapshot(coin)
        bids = (
            snap.get("levels", [[], []])[0]
            if isinstance(snap.get("levels"), list)
            else []
        )
        asks = (
            snap.get("levels", [[], []])[1]
            if isinstance(snap.get("levels"), list)
            else []
        )

        best_bid = None
        best_ask = None

        # levels entries are typically arrays of {"px": "...", "sz": "..."} or similar dicts
        if bids:
            top = bids[0]
            if isinstance(top, dict) and "px" in top:
                try:
                    best_bid = float(top["px"])
                except (TypeError, ValueError):
                    best_bid = None

        if asks:
            top = asks[0]
            if isinstance(top, dict) and "px" in top:
                try:
                    best_ask = float(top["px"])
                except (TypeError, ValueError):
                    best_ask = None

        return best_bid, best_ask


class HyperliquidMidKafkaPublisher:
    """
    Kafka publisher for Hyperliquid mid prices.

    Publishes JSON messages to a topic. Message schema is intentionally simple:
      {"coin":"BTC","mid":12345.6}
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
        topic: str = DEFAULT_KAFKA_TOPIC_MIDS,
        client_id: str = "hl_prices",
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

    def publish_mid(self, msg: HLWsMidPrice, timeout_s: Optional[float] = None) -> None:
        fut = self._producer.send(self._topic, value=[{"Perp": msg.coin}, msg.mid])
        if timeout_s is not None:
            fut.get(timeout=timeout_s)

    def flush(self, timeout_s: float = 10.0) -> None:
        self._producer.flush(timeout=timeout_s)

    def close(self, timeout_s: float = 10.0) -> None:
        try:
            self.flush(timeout_s=timeout_s)
        finally:
            self._producer.close(timeout=timeout_s)


class HyperliquidWS:
    """
    Minimal WebSocket client for streaming mid prices.

    Notes
    -----
    This uses the optional `websocket-client` package (import name: `websocket`).

    It yields mid-price updates via a generator. The generator is blocking and
    will run until the socket closes or an exception occurs.

    References:
    - https://hyperliquid.gitbook.io/hyperliquid-docs (WebSocket subscriptions)
    """

    def __init__(self, ws_url: str = DEFAULT_WS_URL, timeout_s: float = 10.0) -> None:
        if websocket is None:  # pragma: no cover
            raise HyperliquidError(
                "WebSocket streaming requires the optional dependency `websocket-client` "
                "(pip install websocket-client)."
            )
        self._ws_url = ws_url
        self._timeout_s = timeout_s

    @staticmethod
    def _extract_mid_updates(msg: Dict[str, Any]) -> Iterator[HLWsMidPrice]:
        """
        Best-effort parser for Hyperliquid WS allMids payloads.

        We tolerate a few possible envelope shapes because the WS schema may vary:
        - {"channel": "allMids", "data": {"BTC": "123.4", ...}}
        - {"data": {"mids": {"BTC": "123.4", ...}}}
        - {"mids": {"BTC": "123.4", ...}}
        - {"BTC": "123.4", "ETH": "2345.6"}  # plain mapping
        """
        candidates = []

        data = msg.get("data")
        if isinstance(data, dict):
            candidates.append(data)
            mids = data.get("mids")
            if isinstance(mids, dict):
                candidates.append(mids)

        mids = msg.get("mids")
        if isinstance(mids, dict):
            candidates.append(mids)

        candidates.append(msg)

        seen = set()
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue

            emitted_any = False
            for coin, mid in candidate.items():
                if coin in (
                    "channel",
                    "data",
                    "mids",
                    "method",
                    "subscription",
                    "type",
                ):
                    continue
                try:
                    upd = HLWsMidPrice(coin=str(coin), mid=float(mid))
                except (TypeError, ValueError):
                    continue

                key = (upd.coin, upd.mid)
                if key in seen:
                    continue
                seen.add(key)
                emitted_any = True
                yield upd

            if emitted_any:
                return

    def stream_all_mids(self) -> Iterator[HLWsMidPrice]:
        """
        Stream mid prices for all coins.

        Yields
        ------
        HLWsMidPrice updates as they arrive.
        """
        ws = websocket.create_connection(self._ws_url, timeout=self._timeout_s)  # type: ignore[attr-defined]
        try:
            ws.send(
                json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}})
            )
            while True:
                raw = ws.recv()
                try:
                    msg = json.loads(raw)
                except (TypeError, ValueError):
                    continue

                if not isinstance(msg, dict):
                    continue

                if msg.get("channel") == "subscriptionResponse":
                    continue

                for upd in self._extract_mid_updates(msg):
                    yield upd
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def stream_mid(self, coin: str) -> Iterator[HLWsMidPrice]:
        """
        Stream mid prices and filter to a single coin.
        """
        for upd in self.stream_all_mids():
            if upd.coin == coin:
                yield upd


def fetch_hl_mid_prices(
    base_url: str = DEFAULT_HTTP_URL, timeout_s: float = 10.0
) -> Dict[str, float]:
    """
    One-shot helper to fetch all Hyperliquid mid prices.

    This is convenient for scripts and cron jobs.
    """
    return HyperliquidHTTP(base_url=base_url, timeout_s=timeout_s).get_all_mids()


def stream_hl_mid_prices_to_kafka(
    *,
    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_KAFKA_TOPIC_MIDS,
    ws_url: str = DEFAULT_WS_URL,
    timeout_s: float = 10.0,
    limit_updates: Optional[int] = None,
    ping_interval_s: float = 20.0,
    flush_interval_s: float = 1.0,
) -> None:
    """
    Stream Hyperliquid mid prices from WS and publish them to Kafka.

    Common failure modes this hardens against:
      - KafkaProducer send() buffering forever without errors until flush/close
      - WS disconnects / transient network errors
      - subscriptionResponse / heartbeat messages (ignored in WS client)

    Requires:
      - websocket-client
      - kafka-python
    """
    if websocket is None:  # pragma: no cover
        raise HyperliquidError(
            "WebSocket streaming requires the optional dependency `websocket-client` "
            "(pip install websocket-client)."
        )

    publisher = HyperliquidMidKafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )
    ws = HyperliquidWS(ws_url=ws_url, timeout_s=timeout_s)

    n = 0
    last_flush = time.time()
    last_ping = time.time()
    try:
        for upd in ws.stream_all_mids():
            publisher.publish_mid(upd, timeout_s=timeout_s)
            n += 1

            now = time.time()

            if flush_interval_s > 0 and now - last_flush >= flush_interval_s:
                publisher.flush(timeout_s=timeout_s)
                last_flush = now

            if ping_interval_s > 0 and now - last_ping >= ping_interval_s:
                last_ping = now

            if limit_updates is not None and n >= limit_updates:
                break
    finally:
        publisher.close(timeout_s=timeout_s)


def print_hl_mid_prices(
    *,
    ws_url: str = DEFAULT_WS_URL,
    http_url: str = DEFAULT_HTTP_URL,
    timeout_s: float = 10.0,
    use_ws: bool = True,
    limit_updates: Optional[int] = None,
) -> None:
    """
    Fetch and display mid prices.

    By default, uses the WebSocket stream (requires `websocket-client`). If WS is
    unavailable or disabled, falls back to a one-shot HTTP fetch.

    Parameters
    ----------
    ws_url:
        Hyperliquid WS base URL.
    http_url:
        Hyperliquid HTTP base URL.
    timeout_s:
        Socket / HTTP timeout.
    use_ws:
        If True, stream mid prices via WS; otherwise do a one-shot HTTP fetch.
    limit_updates:
        If set, stop after printing this many updates (WS mode only).
    """
    # Prefer WS for streaming if requested, but fall back to HTTP if:
    # - websocket-client isn't installed, OR
    # - the WS connection/subscription fails (common if WS URL or schema changes).
    if use_ws:
        if websocket is None:
            print(
                "WebSocket support not available (missing dependency `websocket-client`). "
                "Falling back to HTTP polling."
            )
        else:
            try:
                ws = HyperliquidWS(ws_url=ws_url, timeout_s=timeout_s)
                n = 0
                for upd in ws.stream_all_mids():
                    print(f"{upd.coin} mid={upd.mid}")
                    n += 1
                    if limit_updates is not None and n >= limit_updates:
                        break
                return
            except Exception as e:
                print(
                    f"WebSocket streaming failed ({type(e).__name__}: {e}). "
                    "Falling back to HTTP polling."
                )

    http = HyperliquidHTTP(base_url=http_url, timeout_s=timeout_s)
    mids = http.get_all_mids()
    for coin in sorted(mids.keys()):
        print(f"{coin} mid={mids[coin]}")


# main
def __main__():
    stream_hl_mid_prices_to_kafka(bootstrap_servers="192.168.1.50:9092")


__main__()
