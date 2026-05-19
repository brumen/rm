from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass
from typing import Callable, Optional
import sys
import logging

logging.basicConfig(level=logging.INFO)  # TODO: Check if this is needed.
logger = logging.getLogger(__name__)

from services.crypto.hl_prices import stream_hl_mid_prices_to_kafka
from services.crypto.hl_trades import stream_hl_trades_to_kafka
from services.crypto.spots import stream_spot_prices_to_kafka


@dataclass
class WorkerState:
    name: str
    thread: threading.Thread
    failed: bool = False
    finished: bool = False
    error: Optional[BaseException] = None
    traceback_str: Optional[str] = None


def _run_worker(state: WorkerState, target: Callable[[], None]) -> None:
    try:
        print(f"[start] {state.name}")
        target()
        state.finished = True
        print(f"[stop] {state.name} exited normally")
    except BaseException as exc:
        state.failed = True
        state.error = exc
        state.traceback_str = traceback.format_exc()
        print(f"[fail] {state.name}: {type(exc).__name__}: {exc}")
        print(state.traceback_str)


def main(host="192.168.1.50", port="9092") -> None:
    workers: list[WorkerState] = []

    def add_worker(name: str, target: Callable[[], None]) -> None:
        state = WorkerState(name=name, thread=threading.Thread(target=lambda: _run_worker(state, target), name=name, daemon=True))  # type: ignore[name-defined]
        workers.append(state)

    bootstrap_servers = f"{host}:{port}"

    add_worker(
        "hl_prices",
        lambda: stream_hl_mid_prices_to_kafka(
            bootstrap_servers=bootstrap_servers,
        ),
    )
    # add_worker(
    #     "hl_trades",
    #     lambda: stream_hl_trades_to_kafka(
    #         bootstrap_servers="192.168.1.50:9092",
    #         coins=["ETH", "BTC", "SEI", "MORPHO", "AAVE", "SOL", "HYPE"],
    #     ),
    # )
    add_worker(
        "spots",
        lambda: stream_spot_prices_to_kafka(
            bootstrap_servers=bootstrap_servers,
            product_ids=[
                "ETH-USD",
                "BTC-USD",
                "SEI-USD",
                "MORPHO-USD",
                "AAVE-USD",
                "SOL-USD",
                "HYPE-USD",
            ],
        ),
    )

    for worker in workers:
        worker.thread.start()

    reported_failures: set[str] = set()

    try:
        while True:
            any_failed = False

            for worker in workers:
                if worker.failed:
                    any_failed = True
                    if worker.name not in reported_failures:
                        reported_failures.add(worker.name)
                        print(
                            f"[reported-failure] {worker.name}: "
                            f"{type(worker.error).__name__ if worker.error else 'UnknownError'}: "
                            f"{worker.error}"
                        )

            if any_failed:
                raise RuntimeError("One or more crypto stream workers failed")

            if all(worker.finished for worker in workers):
                print("[done] all workers exited")
                return

            time.sleep(1.0)
    except KeyboardInterrupt:
        print("[stop] interrupted by user")


if __name__ == "__main__":

    try:
        host = sys.argv[1]
    except Exception as e:
        logger.error(f"Host not specified, or something else: {e}")
        host = "192.168.1.50"

    logger.info(f"Using {host} as broker host")
    main(host=host)
