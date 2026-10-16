# Real-Time Risk Engine Structure (`rt_server/src`)

## Overview

The `rt_server` is a real-time risk engine built in Rust, utilizing the `ractor` framework for actor-based concurrency. It is designed to ingest high-frequency market data and trade updates, calculate risk metrics (e.g., PV, PV01), and publish aggregated portfolio results in real-time.

## Architecture

The system follows a pipeline architecture implemented as a chain of actors. Data flows from "New" (incoming) to "Current" (finalized/accepted state).

### Core Components

#### 1. Entry Point (`main.rs` & `lib.rs`)

- **Initialization**: Loads configuration from `.env`, connects to Kafka (Redpanda), and initializes shared data structures for Markets (`AllMarkets`) and Trades (`TradeRep`).
- **Orchestration**: Calls `engine_letf::start2` to build the actor supervision tree and `start_setup_actor` for dynamic configuration.

#### 2. Actor Pipeline (`engine_letf.rs`, `engine_actor.rs`)

The engine constructs a hierarchical processing chain:

- **`ProcessorNew` (Top Level)**:

  - The entry point for new market data (`NewMarket`) and trade events (`NewTrade`).
  - Manages a "future" market state.
  - Initiates bulk pricing computations.
  - Passes calculated portfolios downstream to `ProcessorMiddle`.

- **`ProcessorMiddle` (Intermediate Layer)**:

  - A chain of actors connecting the top and bottom layers.
  - Manages flow control and state synchronization between the fast-moving "New" state and the stable "Current" state.
  - Handles `NewTradePortfolio` messages from upstream and `Behind` messages from downstream.
  - Ensures consistent state updates by re-triggering calculations if downstream processors report missing trades.
  - States: `Idle`, `CalculatingBulk`, `CalculatingSingle`, `CalculatingBulkMarketSwitch`.

- **`ProcessorCurr` (Bottom Level/Sink)**:
  - Represents the current authoritative state of the portfolio.
  - Receives proposed portfolios from upstream.
  - Validates updates (checking if it is "behind" on any trades).
  - **Result Publishing**: If an update is accepted, it publishes the aggregated portfolio metrics (e.g., JSON payload) to the configured Kafka results topic via `ResultPublisher`.

#### 3. Helper Actors

- **`ProcessorBulk`**: Attached to processors to offload heavy numerical computations (pricing thousands of trades) to avoid blocking the main actor message loop.
- **`SetupActor` (`processor_setup_actor.rs`)**: Listens to a configuration Kafka topic. It allows dynamic updates to the system, such as changing the list of `PricingMetric`s (e.g., switching from just PV to PV + Delta) being calculated by all processors.

#### 4. I/O Handlers

- **`MarketProducer` (`mkt_handler_actor.rs`)**: Consumes market data from Kafka and feeds `ProcessorNew`.
- **`TradeProducer` (`trade_sender.rs`)**: Consumes trade/position updates from Kafka and notifies processors of new trades.
- **`ResultPublisher`**: A Kafka producer wrapper used by `ProcessorCurr` to emit risk results.

## Data Flow

1.  **Ingestion**:
    - `MarketProducer` receives a new market update -> sends to `ProcessorNew`.
    - `TradeProducer` receives a new trade -> sends to all processors.
2.  **Calculation**:
    - `ProcessorNew` (or `ProcessorMiddle`) aggregates trades and current market data.
    - It delegates pricing to `ProcessorBulk`.
    - Once computed, the new portfolio state is passed downstream.
3.  **Synchronization**:
    - `ProcessorCurr` checks if the new portfolio includes all known trades.
    - If `ProcessorCurr` is missing trades (is "behind"), it rejects the update and notifies upstream to re-calculate with the missing data.
4.  **Publication**:
    - Once `ProcessorCurr` accepts a new state, it publishes the results to the output Kafka topic.

## Key Modules

- `engine_letf.rs`: Main actor system builder.
- `processor_*.rs`: Implementation of the specific actor logic for different stages of the pipeline.
- `processor_msg.rs`: Defines the inter-actor messaging protocol (`ProcessorMiddleMessage`, `ProcessorBulkMessage`).
- `market.rs` / `trade.rs`: Core domain traits and data structures.

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
from `processor_curr` and `processor_new`. The URL is also printed at startup and
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
 
