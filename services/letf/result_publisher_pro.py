"""
Professional LETF result viewer (Kafka).

This module provides a richer UI than the existing `result_publisher.py` by using
Textual (a modern TUI framework) to render a professional dashboard with:

- Connection panel and status indicator
- Metric selector (PV, PV01, etc.)
- Start/Stop consumption buttons
- Pause/Resume updates
- Search / filter
- Table view and JSON detail view for selected row
- Export visible results to JSON

It intentionally does not modify the underlying `rm.result_publisher_by_trade`
implementation; instead it consumes from Kafka directly and displays messages.

Run:
    python -m services.letf.result_publisher_pro PV 192.168.1.50
or:
    python services/letf/result_publisher_pro.py PV 192.168.1.50

Dependencies:
    pip install textual confluent-kafka

Notes:
- Topic default is "letf.risk" to match existing publisher.
- Messages are assumed to be JSON payloads.
- If payload is a dict, we try to flatten important fields into columns.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Keep existing convention used in result_publisher.py (kafka vendor six moves)
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
    group_id: str = "letf.result_publisher_pro"
    auto_offset_reset: str = "latest"


@dataclass
class ResultRow:
    ts: datetime
    key: str
    metric: str
    value: Optional[float]
    raw: Any


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _json_loads(b: bytes) -> Any:
    try:
        return json.loads(b.decode("utf-8", errors="replace"))
    except Exception:
        # Best-effort fallback.
        return {"_raw": b.decode("utf-8", errors="replace")}


def _flatten_candidate_fields(obj: Any) -> Dict[str, Any]:
    """
    Normalize common shapes into a dict we can display.
    We handle:
      - dict with keys like id/trade_id/ticker/symbol
      - dict with nested results
      - list/tuple of pairs
    """
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, (list, tuple)):
        # If it's list of 2-tuples, convert to dict.
        try:
            if all(isinstance(x, (list, tuple)) and len(x) == 2 for x in obj):
                return dict(obj)  # type: ignore[arg-type]
        except Exception:
            pass
        return {"_list": obj}
    return {"_value": obj}


class KafkaConsumerWorker:
    """
    Background consumer thread that pushes parsed messages into a queue.
    """

    def __init__(self, cfg: KafkaConfig, metric: str, out_queue: "queue.Queue[ResultRow]"):
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
        self._thread = threading.Thread(target=self._run, name="KafkaConsumerWorker", daemon=True)
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
        return bool(self._thread and self._thread.is_alive() and not self._stop_event.is_set())

    def _run(self) -> None:
        try:
            from confluent_kafka import Consumer  # type: ignore
        except Exception as e:
            # Put a synthetic row describing missing dependency.
            self._q.put(
                ResultRow(
                    ts=_utc_now(),
                    key="system",
                    metric="error",
                    value=None,
                    raw={"error": "Missing dependency: confluent-kafka", "detail": str(e)},
                )
            )
            return

        conf = {
            "bootstrap.servers": f"{self._cfg.host}:{self._cfg.port}",
            "group.id": self._cfg.group_id,
            "enable.auto.commit": True,
            "auto.offset.reset": self._cfg.auto_offset_reset,
        }

        self._consumer = Consumer(conf)
        self._consumer.subscribe([self._cfg.topic])

        self._q.put(
            ResultRow(
                ts=_utc_now(),
                key="system",
                metric="status",
                value=None,
                raw={
                    "status": "connected",
                    "bootstrap": conf["bootstrap.servers"],
                    "topic": self._cfg.topic,
                    "group_id": self._cfg.group_id,
                    "offset_reset": conf["auto.offset.reset"],
                },
            )
        )

        while not self._stop_event.is_set():
            if self._paused.is_set():
                time.sleep(0.1)
                continue

            msg = self._consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                self._q.put(
                    ResultRow(
                        ts=_utc_now(),
                        key="system",
                        metric="error",
                        value=None,
                        raw={"error": str(msg.error())},
                    )
                )
                continue

            payload = _json_loads(msg.value() or b"")
            payload_d = _flatten_candidate_fields(payload)

            key = (
                str(payload_d.get("trade_id"))
                if "trade_id" in payload_d
                else str(payload_d.get("id"))
                if "id" in payload_d
                else str(payload_d.get("ticker"))
                if "ticker" in payload_d
                else str(payload_d.get("symbol"))
                if "symbol" in payload_d
                else str(msg.key().decode("utf-8", errors="replace"))
                if msg.key()
                else "unknown"
            )

            # Try common metric/value shapes.
            value = None
            if self._metric in payload_d:
                value = _safe_float(payload_d.get(self._metric))
            elif "metric" in payload_d and "value" in payload_d:
                # payload carries explicit metric name
                if str(payload_d.get("metric")) == self._metric:
                    value = _safe_float(payload_d.get("value"))
            elif "results" in payload_d and isinstance(payload_d["results"], dict):
                value = _safe_float(payload_d["results"].get(self._metric))

            self._q.put(
                ResultRow(
                    ts=_utc_now(),
                    key=key,
                    metric=self._metric,
                    value=value,
                    raw=payload,
                )
            )

        try:
            self._consumer.close()
        except Exception:
            logger.exception("Error closing consumer (loop exit)")


# --- Textual UI ---

# Textual imports are optional at import-time so the module can still run and
# print an actionable error rather than crashing immediately.
try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Container, Horizontal, Vertical, VerticalScroll
    from textual.message import Message
    from textual.reactive import reactive
    from textual.widgets import (
        Button,
        DataTable,
        Footer,
        Header,
        Input,
        Label,
        Log,
        Select,
        Static,
        TabbedContent,
        TabPane,
    )
except Exception:
    App = object  # type: ignore[misc,assignment]


class StatusPill(Static):
    status: str = reactive("disconnected")

    def render(self) -> str:
        s = self.status
        if s == "connected":
            return "[b green]CONNECTED[/b green]"
        if s == "running":
            return "[b green]RUNNING[/b green]"
        if s == "paused":
            return "[b yellow]PAUSED[/b yellow]"
        if s == "error":
            return "[b red]ERROR[/b red]"
        return "[b red]DISCONNECTED[/b red]"


class ResultApp(App):
    CSS = """
    Screen {
        layout: vertical;
    }

    .topbar {
        height: auto;
        padding: 1 2;
        border: solid $accent;
    }

    .row {
        height: auto;
    }

    .controls Label {
        width: 18;
        content-align: right middle;
        padding-right: 1;
    }

    .controls Input, .controls Select {
        width: 1fr;
    }

    .btnbar {
        height: auto;
        padding-top: 1;
        padding-bottom: 1;
    }

    #results_table {
        height: 1fr;
        border: solid $accent;
    }

    #detail_log {
        height: 1fr;
        border: solid $accent;
    }

    #event_log {
        height: 1fr;
        border: solid $accent;
    }

    .hint {
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("s", "start", "Start"),
        Binding("t", "stop", "Stop"),
        Binding("p", "pause_resume", "Pause/Resume"),
        Binding("e", "export", "Export JSON"),
        Binding("/", "focus_filter", "Focus Filter"),
    ]

    class ExportRequested(Message):
        def __init__(self, path: str) -> None:
            super().__init__()
            self.path = path

    status: str = reactive("disconnected")

    def __init__(self, cfg: KafkaConfig, metric: str) -> None:
        super().__init__()
        self._cfg = cfg
        self._metric = metric

        self._q: "queue.Queue[ResultRow]" = queue.Queue(maxsize=10_000)
        self._worker = KafkaConsumerWorker(cfg=cfg, metric=metric, out_queue=self._q)

        self._rows: List[ResultRow] = []
        self._max_rows = 5_000

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
                yield Select(
                    options=[
                        ("PV", "PV"),
                        ("PV01", "PV01"),
                        ("DV01", "DV01"),
                        ("VAR", "VAR"),
                        ("CUSTOM", "CUSTOM"),
                    ],
                    value=self._metric if self._metric in {"PV", "PV01", "DV01", "VAR"} else "CUSTOM",
                    id="metric_sel",
                )
                yield Label("Custom metric:")
                yield Input(value=self._metric, id="custom_metric_in")

            with Horizontal(classes="row controls"):
                yield Label("Filter:")
                yield Input(placeholder="Type to filter by key or JSON…", id="filter_in")
                yield Label("Status:")
                yield StatusPill(id="status_pill")

            with Horizontal(classes="btnbar"):
                yield Button("Start", id="start_btn", variant="success")
                yield Button("Stop", id="stop_btn", variant="error")
                yield Button("Pause", id="pause_btn", variant="warning")
                yield Button("Clear", id="clear_btn")
                yield Button("Export JSON", id="export_btn", variant="primary")
                yield Label("Hotkeys: s=start, t=stop, p=pause, e=export, /=filter, q=quit", classes="hint")

        with TabbedContent():
            with TabPane("Results"):
                yield DataTable(id="results_table", zebra_stripes=True)
            with TabPane("Details"):
                with VerticalScroll():
                    yield Log(id="detail_log", highlight=True, auto_scroll=False)
            with TabPane("Events"):
                with VerticalScroll():
                    yield Log(id="event_log", highlight=True, auto_scroll=True)

        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#results_table", DataTable)
        table.add_columns("Time (UTC)", "Key", "Metric", "Value")
        table.cursor_type = "row"

        self._set_status("disconnected")
        self.set_interval(0.2, self._drain_queue)

        # Graceful shutdown on SIGINT when running outside textual runner
        try:
            signal.signal(signal.SIGINT, lambda *_: self.exit())  # type: ignore[arg-type]
        except Exception:
            pass

    def _set_status(self, status: str) -> None:
        self.status = status
        pill = self.query_one("#status_pill", StatusPill)
        pill.status = status

    def action_focus_filter(self) -> None:
        self.query_one("#filter_in", Input).focus()

    def action_start(self) -> None:
        self._apply_config_from_inputs()
        self._worker.start()
        self._log_event({"event": "start"})
        self._set_status("running")

    def action_stop(self) -> None:
        self._worker.stop()
        self._log_event({"event": "stop"})
        self._set_status("connected" if self._worker.is_running() else "disconnected")

    def action_pause_resume(self) -> None:
        if self.status == "paused":
            self._worker.resume()
            self._log_event({"event": "resume"})
            self._set_status("running")
            self.query_one("#pause_btn", Button).label = "Pause"
        else:
            self._worker.pause()
            self._log_event({"event": "pause"})
            self._set_status("paused")
            self.query_one("#pause_btn", Button).label = "Resume"

    def action_export(self) -> None:
        export_path = os.path.join(
            os.getcwd(),
            f"letf_results_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
        )
        self.post_message(self.ExportRequested(export_path))

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
        elif bid == "export_btn":
            self.action_export()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        try:
            row = self._rows[event.cursor_row]
        except Exception:
            return
        detail = self.query_one("#detail_log", Log)
        detail.clear()
        detail.write(json.dumps(row.raw, indent=2, default=str))

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id != "metric_sel":
            return
        if event.value != "CUSTOM":
            custom = self.query_one("#custom_metric_in", Input)
            custom.value = str(event.value)

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "filter_in":
            self._refresh_table()

    def on_result_app_export_requested(self, message: "ResultApp.ExportRequested") -> None:
        data = [r.raw for r in self._filtered_rows()]
        try:
            with open(message.path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            self._log_event({"event": "export", "path": message.path, "count": len(data)})
        except Exception as e:
            self._log_event({"event": "export_failed", "path": message.path, "error": str(e)})
            self._set_status("error")

    def _apply_config_from_inputs(self) -> None:
        host = self.query_one("#host_in", Input).value.strip() or DEFAULT_HOST
        topic = self.query_one("#topic_in", Input).value.strip() or DEFAULT_TOPIC

        metric_choice = self.query_one("#metric_sel", Select).value
        custom_metric = self.query_one("#custom_metric_in", Input).value.strip()
        metric = custom_metric if metric_choice == "CUSTOM" else str(metric_choice)

        # Re-create worker if config changed significantly.
        cfg = KafkaConfig(host=host, port=self._cfg.port, topic=topic, group_id=self._cfg.group_id)
        if cfg != self._cfg:
            self._worker.stop()
            self._cfg = cfg
            self._worker = KafkaConsumerWorker(cfg=self._cfg, metric=metric, out_queue=self._q)
        else:
            self._worker.metric = metric

        self._metric = metric

    def _drain_queue(self) -> None:
        drained = 0
        while True:
            try:
                row = self._q.get_nowait()
            except queue.Empty:
                break

            drained += 1
            if row.key == "system" and row.metric in {"status", "error"}:
                if row.metric == "status":
                    self._set_status("connected" if self.status == "disconnected" else self.status)
                elif row.metric == "error":
                    self._set_status("error")
                self._log_event(row.raw)
                continue

            self._rows.append(row)
            if len(self._rows) > self._max_rows:
                self._rows = self._rows[-self._max_rows :]

        if drained:
            self._refresh_table(incremental=True)

    def _filtered_rows(self) -> List[ResultRow]:
        flt = self.query_one("#filter_in", Input).value.strip().lower()
        if not flt:
            return self._rows

        out: List[ResultRow] = []
        for r in self._rows:
            if flt in r.key.lower():
                out.append(r)
                continue
            try:
                raw_s = json.dumps(r.raw, default=str).lower()
            except Exception:
                raw_s = str(r.raw).lower()
            if flt in raw_s:
                out.append(r)
        return out

    def _refresh_table(self, incremental: bool = False) -> None:
        table = self.query_one("#results_table", DataTable)

        # For simplicity and correctness with filtering, rebuild table.
        table.clear()
        for r in self._filtered_rows():
            table.add_row(
                r.ts.strftime("%Y-%m-%d %H:%M:%S"),
                r.key,
                r.metric,
                "" if r.value is None else f"{r.value:,.6f}",
            )

    def _log_event(self, obj: Any) -> None:
        logw = self.query_one("#event_log", Log)
        logw.write(json.dumps({"ts": _utc_now().isoformat(), **_flatten_candidate_fields(obj)}, default=str))

    def _clear(self) -> None:
        self._rows.clear()
        self.query_one("#results_table", DataTable).clear()
        self.query_one("#detail_log", Log).clear()
        self.query_one("#event_log", Log).clear()


def _parse_argv(argv: List[str]) -> Tuple[str, str]:
    # Keep compatibility with existing script behavior:
    # python result_publisher_pro.py PV 192.168.1.107
    try:
        metric = argv[1]
    except Exception:
        metric = "PV"
        logger.info("Metric defaulting to PV")

    try:
        host = argv[2]
    except Exception:
        host = DEFAULT_HOST
        logger.info(f"Host defaulting to {DEFAULT_HOST}")

    return metric, host


def main() -> None:
    metric, host = _parse_argv(sys.argv)
    cfg = KafkaConfig(host=host, port=DEFAULT_BROKER_PORT, topic=DEFAULT_TOPIC)
    try:
        app = ResultApp(cfg=cfg, metric=metric)
    except Exception as e:
        # Provide a plain CLI fallback if textual isn't installed.
        print("Failed to start professional UI. Ensure dependencies are installed:")
        print("  pip install textual confluent-kafka")
        raise

    app.run()


if __name__ == "__main__":
    main()
