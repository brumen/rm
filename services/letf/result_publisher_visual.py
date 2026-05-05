"""
LETF PV visualizer (Kafka).

Consumes Kafka messages from the LETF risk topic and maintains a live view of
PV per trade id. This is a lighter, purpose-built visualization compared to
`result_publisher_pro.py`.

UI (Textual):
- Start / Stop / Pause controls
- Filter by trade id
- Live-updating table: trade_id, PV, last update time
- Sortable by PV (descending) or trade_id
- Sparkline-style mini trend (last N PV updates) rendered as unicode blocks

Run:
    python -m services.letf.result_publisher_visual PV 192.168.1.50
or:
    python services/letf/result_publisher_visual.py PV 192.168.1.50

Dependencies:
    pip install textual confluent-kafka

Notes / assumptions about message shape:
- Messages are JSON.
- Message format is a "double dictionary":
    {
      "PV":   { "2000": 123.4, "2005": -3.2, ... },
      "PV01": { "2000": 0.12,  "2005": 0.05, ... },
      ...
    }
- We display a single metric (argv[1], default "PV") by taking payload[metric]
  and showing one line per trade id (the inner dict keys).
"""

from __future__ import annotations

import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

# Keep existing convention used in other scripts (kafka vendor six moves)
import six.moves

sys.modules["kafka.vendor.six.moves"] = six.moves
sys.path.append("/home/brumen/work/")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DEFAULT_TOPIC = "letf.risk"
DEFAULT_BROKER_PORT = 9092
DEFAULT_HOST = "192.168.1.50"


@dataclass(frozen=True)
class KafkaConfig:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_BROKER_PORT
    topic: str = DEFAULT_TOPIC
    group_id: str = "letf.result_publisher_visual"
    auto_offset_reset: str = "latest"


@dataclass(frozen=True)
class PVUpdate:
    ts: datetime
    trade_id: str
    metric: str
    value: float
    raw: Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_loads(b: bytes) -> Any:
    try:
        return json.loads(b.decode("utf-8", errors="replace"))
    except Exception:
        return {"_raw": b.decode("utf-8", errors="replace")}


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _extract_metric_map(payload: Any, metric: str) -> Dict[str, float]:
    """
    Extract inner trade_id -> value map from the double-dictionary payload.

    Expected payload shape:
        { "<METRIC>": { "<TRADE_ID>": <float>, ... }, ... }

    Returns an empty dict if the metric isn't present or isn't a dict.
    """
    if not isinstance(payload, dict):
        return {}

    inner = payload.get(metric)
    if not isinstance(inner, dict):
        return {}

    out: Dict[str, float] = {}
    for k, v in inner.items():
        fv = _safe_float(v)
        if fv is None:
            continue
        out[str(k)] = fv
    logger.info(f"METRIC: {metric}, VALUE = {out}")
    return out


def _sparkline(values: List[float], width: int = 18) -> str:
    """
    Render a small sparkline from the last values.
    Uses 8-level unicode blocks. Safely handles empty/constant series.
    """
    if not values:
        return ""

    # downsample to width
    if len(values) > width:
        step = len(values) / width
        sampled = []
        for i in range(width):
            sampled.append(values[int(i * step)])
        values = sampled

    vmin = min(values)
    vmax = max(values)
    if vmax == vmin:
        return "▁" * len(values)

    blocks = "▁▂▃▄▅▆▇█"
    out = []
    for v in values:
        idx = int((v - vmin) / (vmax - vmin) * (len(blocks) - 1))
        idx = max(0, min(idx, len(blocks) - 1))
        out.append(blocks[idx])
    return "".join(out)


class KafkaPVWorker:
    """
    Background consumer thread that pushes PVUpdate into a queue.

    Each Kafka message can contain multiple trade entries for a metric; we emit
    one PVUpdate per trade id.
    """

    def __init__(
        self, cfg: KafkaConfig, metric: str, out_queue: "queue.Queue[PVUpdate]"
    ):
        self._cfg = cfg
        self._metric = metric
        self._q = out_queue

        self._stop_event = threading.Event()
        self._paused = threading.Event()
        self._paused.clear()

        self._thread: Optional[threading.Thread] = None
        self._consumer = None

    @property
    def metric(self) -> str:
        return self._metric

    @metric.setter
    def metric(self, new_metric: str) -> None:
        self._metric = new_metric

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="KafkaPVWorker", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            if self._consumer is not None:
                self._consumer.close()
        except Exception:
            logger.exception("Error closing consumer")

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    def is_running(self) -> bool:
        return bool(
            self._thread and self._thread.is_alive() and not self._stop_event.is_set()
        )

    def _run(self) -> None:
        try:
            from confluent_kafka import Consumer  # type: ignore
        except Exception as e:
            logger.error("Missing dependency confluent-kafka: %s", e)
            return

        conf = {
            "bootstrap.servers": f"{self._cfg.host}:{self._cfg.port}",
            "group.id": self._cfg.group_id,
            "enable.auto.commit": True,
            "auto.offset.reset": self._cfg.auto_offset_reset,
        }

        self._consumer = Consumer(conf)
        self._consumer.subscribe([self._cfg.topic])

        while not self._stop_event.is_set():
            if self._paused.is_set():
                time.sleep(0.1)
                continue

            msg = self._consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Kafka error: %s", msg.error())
                continue

            payload = _json_loads(msg.value() or b"")
            metric_map = _extract_metric_map(payload, self._metric)
            if not metric_map:
                continue

            now = _utc_now()
            for trade_id, v in metric_map.items():
                try:
                    self._q.put_nowait(
                        PVUpdate(
                            ts=now,
                            trade_id=str(trade_id),
                            metric=self._metric,
                            value=v,
                            raw=payload,
                        )
                    )
                except queue.Full:
                    # If the UI can't keep up, drop updates.
                    break

        try:
            self._consumer.close()
        except Exception:
            logger.exception("Error closing consumer (loop exit)")


# --- Textual UI ---
try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Container, Horizontal, Vertical
    from textual.reactive import reactive
    from textual.widgets import (
        Button,
        DataTable,
        Footer,
        Header,
        Input,
        Label,
        Select,
        Static,
    )
except Exception:
    App = object  # type: ignore[misc,assignment]


class StatusPill(Static):
    status: str = reactive("disconnected")

    def render(self) -> str:
        s = self.status
        if s == "running":
            return "[b green]RUNNING[/b green]"
        if s == "paused":
            return "[b yellow]PAUSED[/b yellow]"
        if s == "error":
            return "[b red]ERROR[/b red]"
        return "[b red]STOPPED[/b red]"


class PVVisualApp(App):
    CSS = """
    Screen { layout: vertical; }
    .topbar { height: auto; padding: 1 2; border: solid $accent; }
    .row { height: auto; }
    .controls Label { width: 16; content-align: right middle; padding-right: 1; }
    .controls Input, .controls Select { width: 1fr; }
    #pv_table { height: 1fr; border: solid $accent; }
    .hint { color: $text-muted; }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "start", "Start"),
        Binding("t", "stop", "Stop"),
        Binding("p", "pause_resume", "Pause/Resume"),
        Binding("/", "focus_filter", "Focus Filter"),
    ]

    status: str = reactive("stopped")

    def __init__(self, cfg: KafkaConfig, metric: str) -> None:
        super().__init__()
        self._cfg = cfg
        self._metric = metric

        self._q: "queue.Queue[PVUpdate]" = queue.Queue(maxsize=50_000)
        self._worker = KafkaPVWorker(cfg=cfg, metric=metric, out_queue=self._q)

        # per-trade state
        self._last: Dict[str, PVUpdate] = {}
        self._hist: Dict[str, Deque[float]] = {}
        self._hist_len = 60

        self._sort_mode = "PV_DESC"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Container(classes="topbar"):
            with Horizontal(classes="row controls"):
                yield Label("Broker host:")
                yield Input(value=self._cfg.host, id="host_in")
                yield Label("Topic:")
                yield Input(value=self._cfg.topic, id="topic_in")

            with Horizontal(classes="row controls"):
                yield Label("Metric:")
                yield Input(value=self._metric, id="metric_in")
                yield Label("Sort:")
                yield Select(
                    options=[
                        ("Value ↓", "PV_DESC"),
                        ("Value ↑", "PV_ASC"),
                        ("Trade ID", "TRADE_ID"),
                    ],
                    value=self._sort_mode,
                    id="sort_sel",
                )

            with Horizontal(classes="row controls"):
                yield Label("Filter:")
                yield Input(placeholder="Filter trade_id…", id="filter_in")
                yield Label("Status:")
                yield StatusPill(id="status_pill")

            with Horizontal(classes="row"):
                yield Button("Start", id="start_btn", variant="success")
                yield Button("Stop", id="stop_btn", variant="error")
                yield Button("Pause", id="pause_btn", variant="warning")
                yield Button("Clear", id="clear_btn")
                yield Label(
                    "Hotkeys: s=start, t=stop, p=pause, /=filter, q=quit",
                    classes="hint",
                )

        yield DataTable(id="pv_table", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#pv_table", DataTable)
        table.add_columns("Trade ID", self._metric, "Last Update (UTC)", "Trend")
        table.cursor_type = "row"

        self._set_status("stopped")
        self.set_interval(0.2, self._drain_queue)

        try:
            signal.signal(signal.SIGINT, lambda *_: self.exit())  # type: ignore[arg-type]
        except Exception:
            pass

    def _set_status(self, status: str) -> None:
        self.status = status
        self.query_one("#status_pill", StatusPill).status = status

    def action_focus_filter(self) -> None:
        self.query_one("#filter_in", Input).focus()

    def action_start(self) -> None:
        self._apply_config_from_inputs()
        self._worker.start()
        self._set_status("running")
        self.query_one("#pause_btn", Button).label = "Pause"

    def action_stop(self) -> None:
        self._worker.stop()
        self._set_status("stopped")

    def action_pause_resume(self) -> None:
        if self.status == "paused":
            self._worker.resume()
            self._set_status("running")
            self.query_one("#pause_btn", Button).label = "Pause"
        else:
            self._worker.pause()
            self._set_status("paused")
            self.query_one("#pause_btn", Button).label = "Resume"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id
        if bid == "start_btn":
            self.action_start()
        elif bid == "stop_btn":
            self.action_stop()
        elif bid == "pause_btn":
            self.action_pause_resume()
        elif bid == "clear_btn":
            self._clear()

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "filter_in":
            self._refresh_table()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "sort_sel":
            self._sort_mode = str(event.value)
            self._refresh_table()

    def _apply_config_from_inputs(self) -> None:
        host = self.query_one("#host_in", Input).value.strip() or DEFAULT_HOST
        topic = self.query_one("#topic_in", Input).value.strip() or DEFAULT_TOPIC
        metric = self.query_one("#metric_in", Input).value.strip() or "PV"

        cfg = KafkaConfig(
            host=host, port=self._cfg.port, topic=topic, group_id=self._cfg.group_id
        )
        if cfg != self._cfg:
            self._worker.stop()
            self._cfg = cfg
            self._worker = KafkaPVWorker(
                cfg=self._cfg, metric=metric, out_queue=self._q
            )
        else:
            self._worker.metric = metric

        if metric != self._metric:
            self._metric = metric
            table = self.query_one("#pv_table", DataTable)
            table.clear(columns=True)
            table.add_columns("Trade ID", self._metric, "Last Update (UTC)", "Trend")

    def _drain_queue(self) -> None:
        changed = 0
        while True:
            try:
                upd = self._q.get_nowait()
            except queue.Empty:
                break

            changed += 1
            self._last[upd.trade_id] = upd
            h = self._hist.get(upd.trade_id)
            if h is None:
                h = deque(maxlen=self._hist_len)
                self._hist[upd.trade_id] = h
            h.append(upd.value)

        if changed:
            self._refresh_table()

    def _iter_filtered(self) -> List[Tuple[str, PVUpdate]]:
        flt = self.query_one("#filter_in", Input).value.strip().lower()
        items = list(self._last.items())

        if flt:
            items = [(tid, upd) for (tid, upd) in items if flt in tid.lower()]

        if self._sort_mode == "TRADE_ID":
            items.sort(key=lambda x: x[0])
        elif self._sort_mode == "PV_ASC":
            items.sort(key=lambda x: x[1].value)
        else:
            items.sort(key=lambda x: x[1].value, reverse=True)

        return items

    def _refresh_table(self) -> None:
        table = self.query_one("#pv_table", DataTable)
        table.clear()

        for trade_id, upd in self._iter_filtered():
            trend = _sparkline(list(self._hist.get(trade_id, [])))
            table.add_row(
                trade_id,
                f"{upd.value:,.6f}",
                upd.ts.strftime("%Y-%m-%d %H:%M:%S"),
                trend,
            )

    def _clear(self) -> None:
        self._last.clear()
        self._hist.clear()
        self.query_one("#pv_table", DataTable).clear()


def _parse_argv(argv: List[str]) -> Tuple[str, str]:
    # python result_publisher_visual.py PV 192.168.1.107
    try:
        metric = argv[1]
    except Exception as e:
        metric = "PV"
        logger.info("Metric defaulting to PV (%s)", e)

    try:
        host = argv[2]
    except Exception as e:
        host = DEFAULT_HOST
        logger.info("Host defaulting to %s (%s)", DEFAULT_HOST, e)

    return metric, host


def main() -> None:
    metric, host = _parse_argv(sys.argv)
    cfg = KafkaConfig(host=host, port=DEFAULT_BROKER_PORT, topic=DEFAULT_TOPIC)

    try:
        app = PVVisualApp(cfg=cfg, metric=metric)
    except Exception:
        print("Failed to start UI. Ensure dependencies are installed:")
        print("  pip install textual confluent-kafka")
        raise

    app.run()


if __name__ == "__main__":
    main()
