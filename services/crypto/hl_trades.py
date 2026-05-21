"""
Hyperliquid trades connector (most recent + streaming).

This module provides:
- HTTP: fetch recent trades for a coin
- WebSocket: subscribe to streaming trades

The WebSocket stream is the right choice for "most recent (or streaming) trades".

References (public):
- https://hyperliquid.gitbook.io/hyperliquid-docs (API docs)
"""

from __future__ import annotations

import sys
import json
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional

import requests
import websocket

try:
    from kafka import KafkaProducer  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    KafkaProducer = None  # type: ignore[assignment]


DEFAULT_HTTP_URL = "https://api.hyperliquid.xyz"
DEFAULT_WS_URL = "wss://api.hyperliquid.xyz/ws"

DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC_TRADES = "letf.positions"  # "crypto.hl.trades"


class HyperliquidError(RuntimeError):
    pass


@dataclass(frozen=True)
class HLTrade:
    """
    Normalized trade event.

    Fields are best-effort based on Hyperliquid's public trade schema.
    """

    underlying: str
    px: float
    amount: float
    side: str  # "B" / "S" (best-effort)
    ts_ms: int
    trade_id: Optional[str] = None
    leverage: Optional[float] = None


class PublishGate:
    """
    Allows one publish each time the user presses Enter.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._permits = 0

    def allow_one(self) -> None:
        with self._lock:
            self._permits += 1

    def consume_one_if_available(self) -> bool:
        with self._lock:
            if self._permits <= 0:
                return False
            self._permits -= 1
            return True


def _require_kafka() -> None:
    if KafkaProducer is None:  # pragma: no cover
        raise HyperliquidError(
            "Kafka publishing requires the optional dependency `kafka-python` "
            "(pip install kafka-python)."
        )


def _json_serializer(v: Any) -> bytes:
    return json.dumps(v, separators=(",", ":"), sort_keys=True).encode("utf-8")


class HyperliquidTradesHTTP:
    """
    Minimal HTTP wrapper for fetching recent trades.
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

    @staticmethod
    def _leverage(coin: str):
        return HyperliquidTradesWS._leverage(coin)

    def recent_trades(self, coin: str, limit: int = 200) -> List[HLTrade]:
        """
        Fetch recent trades for a coin.

        Endpoint: POST /info { "type": "recentTrades", "coin": "<coin>" }

        The API returns a list of trade dicts. We normalize into `HLTrade`.
        """
        if limit <= 0:
            return []

        data = self._post("/info", {"type": "recentTrades", "coin": coin})

        if not isinstance(data, list):
            raise HyperliquidError(
                f"Unexpected recentTrades response type: {type(data)}"
            )

        out: List[HLTrade] = []
        for t in data[:limit]:
            if not isinstance(t, dict):
                continue
            try:
                out.append(
                    HLTrade(
                        underlying=coin,
                        px=float(t.get("px")),
                        amount=float(t.get("sz")),
                        side=str(t.get("side") or t.get("dir") or ""),
                        ts_ms=int(t.get("time") or t.get("ts") or 0),
                        trade_id=(
                            str(t.get("tid") or t.get("hash") or t.get("id"))
                            if (t.get("tid") or t.get("hash") or t.get("id"))
                            else None
                        ),
                        leverage=self._leverage(coin),
                    )
                )
            except (TypeError, ValueError):
                continue
        return out


class HyperliquidTradesKafkaPublisher:
    """
    Kafka publisher for Hyperliquid trade events.

    Publishes JSON messages to a topic. Schema mirrors `HLTrade`:
      {"coin":"BTC","px":...,"sz":...,"side":"B","ts_ms":...,"trade_id":"..."}
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
        topic: str = DEFAULT_KAFKA_TOPIC_TRADES,
        client_id: str = "hl_trades",
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

    def publish_trade(self, trade: HLTrade) -> None:
        self._producer.send(self._topic, value={"Perp": asdict(trade)})

    def flush(self, timeout_s: float = 10.0) -> None:
        self._producer.flush(timeout=timeout_s)

    def close(self, timeout_s: float = 10.0) -> None:
        try:
            self.flush(timeout_s=timeout_s)
        finally:
            self._producer.close(timeout=timeout_s)


class HyperliquidTradesWS:
    """
    WebSocket client to stream trades.
    """

    def __init__(
        self, ws_url: str = DEFAULT_WS_URL, ping_interval_s: float = 20.0
    ) -> None:
        self._ws_url = ws_url
        self._ping_interval_s = ping_interval_s

    def _build_subscribe_msg(self, coin: str) -> Dict[str, Any]:
        return {"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}

    def stream_trades(
        self,
        coins: Iterable[str],
        on_trade: Callable[[HLTrade], None],
        on_error: Optional[Callable[[Exception], None]] = None,
        on_raw: Optional[Callable[[Dict[str, Any]], None]] = None,
        run_forever: bool = True,
    ) -> None:
        coins_l = [c for c in coins]
        if not coins_l:
            raise ValueError("coins cannot be empty")

        backoff_s = 1.0
        while True:
            try:
                self._run_once(coins_l=coins_l, on_trade=on_trade, on_raw=on_raw)
                time.sleep(2.0)
                if not run_forever:
                    return
            except Exception as e:
                if on_error:
                    on_error(e)
                if not run_forever:
                    raise
            time.sleep(backoff_s)
            backoff_s = min(backoff_s * 2.0, 30.0)

    @staticmethod
    def _leverage(coin: str):
        if coin == "BTC":
            return 40.0
        if coin == "ETH":
            return 20.0
        return 10.0

    def _run_once(
        self,
        coins_l: List[str],
        on_trade: Callable[[HLTrade], None],
        on_raw: Optional[Callable[[Dict[str, Any]], None]],
    ) -> None:
        ws = websocket.create_connection(self._ws_url, enable_multithread=True)
        try:
            for coin in coins_l:
                ws.send(json.dumps(self._build_subscribe_msg(coin)))

            last_ping = time.time()

            while True:
                now = time.time()
                if now - last_ping >= self._ping_interval_s:
                    try:
                        ws.ping()
                    except Exception:
                        break
                    last_ping = now

                raw = ws.recv()
                if raw is None:
                    break

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if on_raw and isinstance(msg, dict):
                    on_raw(msg)

                if not isinstance(msg, dict):
                    continue

                channel = msg.get("channel") or msg.get("type")
                data = msg.get("data")

                if channel != "trades":
                    continue
                if not isinstance(data, list):
                    continue

                for t in data:
                    if not isinstance(t, dict):
                        continue
                    coin = str(t.get("coin") or "")
                    if not coin:
                        continue
                    try:
                        trade = HLTrade(
                            underlying=coin,
                            px=float(t.get("px")),
                            amount=float(t.get("sz")),
                            side=str(t.get("side") or t.get("dir") or ""),
                            ts_ms=int(t.get("time") or t.get("ts") or 0),
                            trade_id=(
                                str(t.get("tid") or t.get("hash") or t.get("id"))
                                if (t.get("tid") or t.get("hash") or t.get("id"))
                                else None
                            ),
                            leverage=self._leverage(coin),
                        )
                    except (TypeError, ValueError):
                        continue
                    on_trade(trade)
        finally:
            try:
                ws.close()
            except Exception:
                pass


def _start_keyboard_gate_thread(gate: PublishGate) -> threading.Thread:
    def _reader() -> None:
        print("Press Enter to allow the next trade to be published to Kafka.")
        while True:
            try:
                input()
            except EOFError:
                break
            gate.allow_one()
            print("[gate] next trade will be published")

    t = threading.Thread(target=_reader, name="hl_trades_keyboard_gate", daemon=True)
    t.start()
    return t


def stream_hl_trades_to_kafka(
    coins: Iterable[str],
    *,
    bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_KAFKA_TOPIC_TRADES,
    ws_url: str = DEFAULT_WS_URL,
    ping_interval_s: float = 20.0,
    run_forever: bool = True,
) -> None:
    """
    Stream trades from WS and publish them to Kafka.

    Trades are ignored by default.
    Press Enter in the terminal to allow exactly one next trade through to Kafka.
    """
    publisher = HyperliquidTradesKafkaPublisher(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )
    gate = PublishGate()
    _keyboard_thread = _start_keyboard_gate_thread(gate)

    def _on_trade(tr: HLTrade) -> None:
        if gate.consume_one_if_available():
            publisher.publish_trade(tr)
            print(
                f"[published] {tr.underlying} px={tr.px} amount={tr.amount} side={tr.side}"
            )

    def _on_error(e: Exception) -> None:
        try:
            publisher.flush(timeout_s=5.0)
        except Exception:
            pass

    ws = HyperliquidTradesWS(ws_url=ws_url, ping_interval_s=ping_interval_s)
    try:
        ws.stream_trades(
            coins=coins, on_trade=_on_trade, on_error=_on_error, run_forever=run_forever
        )
    finally:
        publisher.close(timeout_s=10.0)


def fetch_recent_hl_trades(
    coin: str,
    limit: int = 200,
    base_url: str = DEFAULT_HTTP_URL,
    timeout_s: float = 10.0,
) -> List[HLTrade]:
    """
    One-shot helper to fetch recent trades for a coin.
    """
    return HyperliquidTradesHTTP(base_url=base_url, timeout_s=timeout_s).recent_trades(
        coin=coin, limit=limit
    )


def main(host="192.168.1.50"):
    stream_hl_trades_to_kafka(
        bootstrap_servers=f"{host}:9092",
        coins=["ETH", "BTC", "SEI", "MORPHO", "AAVE", "SOL", "HYPE"],
    )


if __name__ == "__main__":

    try:
        host = sys.argv[1]
    except Exception as e:
        host = "192.168.1.50"

    main(host=host)
