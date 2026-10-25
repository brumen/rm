# Real-Time Risk Engine Structure (`rt_server/src`)

## Overview

The `rt_server` is a real-time risk engine built in Rust, utilizing the `ractor` framework for actor-based concurrency. It is designed to ingest high-frequency market data and trade updates, calculate risk metrics (e.g., PV, PV01), and publish aggregated portfolio results in real-time.

## Architecture

The system follows a pipeline architecture implemented as a chain of actors. Data flows from "New" (incoming) to "Current" (finalized/accepted state).

### Core Components

#### 1. Entry Point (`main.rs` & `lib.rs`)

- **Initialization**: Loads configuration from `.env`, connects to Kafka (Redpanda), and initializes shared data structures for Markets (`AllMarkets`) and Trades (`TradeRep`).
- **Orchestration**: Calls `engine_letf::start2` to build the actor supervision tree and `start_setup_actor` for dynamic configuration.

#### 2. Adaptive actor pipeline

`processor_flow.rs` implements the shared scheduler for the New, Middle and
Current roles. New accepts complete market snapshots and prices immutable,
versioned trade values. Middle and Current follow upstream priced coverage with
adaptive pacing. Current accepts portfolios with fewer than ten locally priced
trade versions missing, reprices the difference on the accepted market, and
publishes metric totals to the existing Kafka results topic. Single-buffer mode
uses upstream results without independently pricing incoming trades.

Pricing runs outside actor handlers with one batch in flight per processor.
Sequential mode prices one trade at a time; the parallel modes have at most 16
trade computations in flight. Kafka publication has one worker in flight and one
latest pending state. Portfolio/configuration messages can be handled while
pricing or publication waits. Market changes and metric changes invalidate old
pricing completions; those results cannot overwrite a newer accepted state.

`processor_msg.rs` defines the internal typed protocol. Kafka message keys and
values are unchanged. `processor_curr.rs` contains the Kafka publisher and sink
acceptance threshold; `processor_bulk.rs` contains the bounded pricing worker.
The former New/Middle processor modules retain aliases for the shared processor.

#### 3. Admission and scheduling

Trade ingress sends to New first, then Middle processors from top to bottom,
then Current. Each actor has a separate admission semaphore. A permit remains
held through the actor mailbox, pending queue and pricing until the trade version
has been priced, covered by a successful equal/newer snapshot, or explicitly
reported as unpriceable. Throttling never drops a trade to make room. Events carry
immutable trade values, so an update in the shared registry cannot change an
already queued event. Newer versions replace earlier contributions rather than
adding the same position twice.

When admission is full, ingress waits and Kafka retains the backlog. Trade
consumer offsets are stored only after every processor has admitted the event;
this is **not** a new exactly-once or end-to-end publication guarantee. Consumer
groups still use the existing per-session UUID policy. Malformed input is logged
and skipped. Unpriceable versions are logged, excluded from successful coverage,
and retained in the actor's latest trade state for retry on a new market or
metric set.

Each edge permits one outstanding portfolio snapshot; while it is occupied, the
sender retains its current state and defers further incremental pricing. Raw
market updates are all applied before coalescing complete markets into one latest
pending snapshot. Actor handlers never wait for an output permit. One timer per
actor drives a small token bucket; no four-second sleeps remain in handlers.

#### 4. Adaptive pacing configuration

`throttle.rs` measures pricing time per trade, publishing time, advancing upstream
coverage and the actual lead in successfully priced trade **versions**. Monotonic
clocks and time-weighted EWMAs exclude intentional waits and avoid interpreting
idle periods as processing capacity. Comparable rejected portfolios update the
controller too; duplicate snapshots do not reset its stall timer.

The target rate is upstream coverage progress plus a correction for the lead
error over the recovery interval, capped by measured local pricing capacity.
A one-trade deadband and a smoothed rate reduce oscillation. Normal pacing
preserves headroom below the sink's ten-trade rejection threshold. If upstream
makes no progress while work is pending, the actor logs a warning and uses a
conservative fallback capped at 10% of the configured initial rate. This relaxes
the lead target to avoid indefinite waiting; incoming portfolio messages remain
serviceable. Worker timeouts and bounded publication retries prevent hung I/O
from occupying the only worker indefinitely.

Optional environment variables (validated at startup):

| Variable | Default | Meaning |
| --- | ---: | --- |
| `RT_THROTTLE_CAPACITY` | 256 | Outstanding trade deliveries per actor, 1–65536 |
| `RT_THROTTLE_BURST` | 4 | Maximum incremental batch/burst, 1–8, capped by capacity |
| `RT_THROTTLE_TARGET_LEAD` | 4 | Desired lead, 1–7 trade versions |
| `RT_THROTTLE_EWMA_MS` | 1000 | Performance smoothing time |
| `RT_THROTTLE_RECOVERY_MS` | 500 | Lead correction horizon |
| `RT_THROTTLE_STALL_MS` | 2000 | Upstream progress timeout while work is pending |
| `RT_THROTTLE_WORKER_TIMEOUT_MS` | 30000 | Deadline for one pricing/publication job |
| `RT_THROTTLE_INITIAL_RATE` | 2000 | Initial trades/second before measurements |

Duration settings accept integral milliseconds from 1 through 3600000. Rates
must be finite, positive and at most 1000000. These defaults need tuning with
representative workload replay; no production latency guarantee is implied.

The `/portfolio_results` page includes an adaptive pacing table for all pipeline
stages. Its data/SSE endpoints include `throttle` statistics: pending count and
oldest age, lead and target, rate, pricing/publishing times, upstream rate, stall
status and failed-price count. Diagnostic captures are coalesced to at most ten
per second per active actor, and settle after the final state change.

## Data Flow

1. Kafka ingress admits versioned trade values with bounded per-actor credits.
2. New prices the latest complete market; Middle and Current pace incremental
   work using recent service times and upstream coverage.
3. Incoming portfolios update the controller and replace eligible state, with
   missing local contributions repriced by a worker on the accepted market.
4. Current publishes the latest aggregate state through a bounded worker.

## Live Processor Market Map

With the server running, open `http://<diagnostics-host>:3000/processor_market_map`
using the address printed by the diagnostics startup log (the local IP detected by
`main.rs`). The page displays processor-to-market assignments, with filtering,
processor/market counts, highlighted assignment changes, and connection status.
It reconnects automatically if the diagnostics connection drops.

- `/processor_market_map/data` returns the current mapping as a JSON object.
- `/processor_market_map/events` streams JSON snapshots using server-sent events
  every 500 ms, including an immediate initial snapshot. Changes between samples
  may not be visible; this is a live sampled view, not an event history.
- The existing `/market_map` text endpoint remains unchanged.

The display uses the diagnostics server's existing shared `AllMarkets` instance
and requires no additional frontend build or external assets. Like the other
diagnostics endpoints, it has no authentication; expose port 3000 only to trusted
users or protect it with an authenticated proxy.

## Live Portfolio Results

Open `http://<diagnostics-host>:3000/portfolio_results` to view portfolio results
from `processor_curr`, `processor_new` and the middle stages. The URL is also printed at startup and
linked from the processor market map. The display streams snapshots every 500 ms,
with metric totals, trade counts, associated market, processor state, and result
capture time. Filter by results topic, processor, or free text, and expand
per-trade results to inspect individual values (up to 500 matching rows at once).

Results are captured directly from the actor portfolios on pricing updates,
accepted portfolio replacements, metric changes, and bulk resets. They are grouped
by Kafka results topic so the two engine pipelines do not overwrite each other.
`processor_new` results are provisional and clear while a new bulk calculation is
pending; `processor_curr` shows its current accepted/priced portfolio. These are
actor-state snapshots, not confirmations of Kafka delivery. Non-finite values and
totals are represented as `null` in JSON and `N/A` in the page.

- `/portfolio_results/data` returns the latest snapshots as JSON.
- `/portfolio_results/events` streams the same data using server-sent events.
- The existing `/portfolio` text endpoint is unchanged.

The page requires no frontend build or external assets and uses the same
unauthenticated diagnostics server; keep it restricted to trusted users.


## Kafka connection functions 

- `portfolio_sender.rs`: contains two functions, the more importat is  connect_with_retries_rd, which connects to kafka stream and reads from it. 
- `publish.rs`: contains the function connect_with_retries_producer_rd - this creates a kafka producer which can publish to a kafka topic.
 
