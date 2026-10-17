# Real-Time Risk Engine

`rt_server` is a Rust service for continuously valuing portfolios as market data
and trade updates arrive. It consumes Kafka-compatible streams, coordinates
pricing through asynchronous actors, and publishes portfolio results for
downstream consumers.

The engine separates newly calculated, provisional portfolios from the current
accepted portfolio. This lets market repricing and incremental trade processing
run through a pipeline rather than a single processing loop.

## Features

- **Actor-based processing:** Tokio and `ractor` coordinate ingestion, pricing,
  portfolio synchronization, and publication.
- **Streaming inputs and outputs:** Kafka/Redpanda integration for market data,
  positions, metric configuration, and pricing results.
- **Configurable execution:** Single- or double-buffer operation, with sequential,
  concurrent single-thread, and parallel pricing strategies.
- **Extensible pricing:** Trade and market traits support different instruments
  and pricing implementations. The current executable uses an LETF-oriented
  model with leveraged ETF, futures, cash, and perpetual trade variants.
- **Risk post-processing:** NAV aggregation and a delta-normal VaR processor
  using rolling market returns and covariance estimates.
- **Live diagnostics:** Browser dashboards for processor-to-market assignments
  and portfolio results, plus JSON and server-sent event endpoints.

## Architecture

```text
Market stream --> MarketProducer --> ProcessorNew
                                         |
                                 Optional ProcessorMiddle chain
                                         |
Position stream --> TradeProducer --> ProcessorCurr --> Results stream
                         |                                   |
                         +--> ProcessorNew / middle actors   +--> NAV / VaR

Setup stream --> SetupActor --> Pricing metric configuration
```

- **`ProcessorNew`** handles incoming markets and trades, starts bulk repricing,
  and proposes portfolios to the next processor.
- **`ProcessorMiddle`** provides optional intermediate stages that reconcile
  portfolios and missing trades between upstream and downstream processors.
- **`ProcessorCurr`** evaluates incoming portfolios, reconciles missing trades
  when possible, and publishes accepted results.
- **`ProcessorBulk`** performs bulk pricing work separately from the main
  processor actors.

Market versions and trades are held in shared concurrent collections. Pricing
results are organized by metric; the current publisher sends the metric as the
Kafka message key and a JSON map of result identifiers to values as the payload.
The identifiers can represent trades or aggregated factors, depending on the
metric and pricing implementation.

## Getting Started

### Requirements

- A Rust toolchain with Cargo and the native build dependencies needed by the
  Kafka and TLS libraries on your platform.
- A reachable Kafka-compatible broker, such as Kafka or Redpanda.
- Market, position, and setup-message producers using the formats expected by
  the selected trade and market implementations.

### Configuration

Run the service from `rt_server/`. Configure its environment directly or create
a local `.env` file there. The current executable reads these variables:

| Variable | Purpose |
| --- | --- |
| `HOST`, `KAFKA_PORT` | Broker host and port used to construct the main Kafka bootstrap address. |
| `POSITIONS_TOPIC` | Incoming position/trade updates. |
| `MKT_RAW_TOPIC` | Incoming market updates. |
| `RESULTS_TOPIC` | Primary pricing results topic. |
| `SETUP_TOPIC` | Runtime pricing-metric configuration. |
| `DEBUG_LEVEL` | `info` or `debug`. |
| `PRICING_MODE` | `sequential`, `parallel_one_thread`, or `parallel`. |
| `OPERATING_MODE` | `single` for single buffering; `double` selects double buffering. |
| `MARKET_PORT`, `PRICING_PORT` | Required by the current startup code, but not used to bind diagnostics. |

The processors start without pricing metrics. Send a setup message to
`SETUP_TOPIC` to select them, for example:

```json
{"Metrics": ["PV", "PV01"]}
```

Keep local environment files and credentials out of version control.

### Build and Run

From the repository root:

```bash
cd rt_server
cargo build --release
cargo run --release
```

The startup wiring is development-oriented: it launches the configured pipeline
and an additional single-buffer pipeline publishing to `letf.risk2`. Review
[`main.rs`](rt_server/src/main.rs), including the setup consumers' broker-address
wiring, before adapting the service to another environment.

## Live Dashboards

Diagnostics bind to the detected local IP on port **3000**. The complete URLs
are printed at startup; the service is not necessarily bound to `localhost`.

| Path | Display |
| --- | --- |
| `/processor_market_map` | Processor-to-market assignments, filtering, counts, and highlighted changes. |
| `/portfolio_results` | `processor_curr` and `processor_new` metric totals, trade counts, markets, states, capture times, and expandable individual values. |

Both pages receive snapshots every **500 ms**, reconnect automatically, and
require no frontend build or external assets. Append `/data` for JSON snapshots
or `/events` for the corresponding server-sent event stream.

Portfolio results are separated by results topic so the two pipelines remain
distinct. `processor_new` results are provisional and clear during bulk
recomputation. The dashboards show sampled actor state, not an event history or
confirmation of Kafka delivery.

**Security:** Diagnostics have no authentication. Restrict port 3000 to trusted
users or place the service behind an authenticated proxy.

## Development

Run commands from `rt_server/`:

```bash
cargo check
cargo test
```

Focused checks for the live diagnostics:

```bash
cargo test portfolio_results
cargo test test_processor_market
```

The project is under active development. Metric support depends on the selected
trade implementation; some paths, including parts of `PnL` handling, remain
unimplemented. Validate pricing, synchronization, and deployment behavior for
your use case before relying on the outputs.

## Code Guide

| Location | Responsibility |
| --- | --- |
| [`rt_server/src/main.rs`](rt_server/src/main.rs) | Environment configuration and service startup. |
| [`rt_server/src/engine_letf.rs`](rt_server/src/engine_letf.rs) | Actor-pipeline construction. |
| [`rt_server/src/processor_msg.rs`](rt_server/src/processor_msg.rs) | Messages exchanged between processors. |
| [`rt_server/src/pricer.rs`](rt_server/src/pricer.rs) | Pricing traits and metric definitions. |
| [`rt_server/src/portfolio.rs`](rt_server/src/portfolio.rs) | Portfolio representations and result aggregation. |
| [`rt_server/src/trades/`](rt_server/src/trades/) and [`rt_server/src/markets/`](rt_server/src/markets/) | Instrument and market implementations. |
| [`rt_server/src/postprocs/`](rt_server/src/postprocs/) | NAV and VaR consumers. |
| [`rt_server/src/processor_setup.rs`](rt_server/src/processor_setup.rs) | HTTP diagnostics and live dashboard endpoints. |

See the [detailed engine README](rt_server/src/README.md) for more information.
