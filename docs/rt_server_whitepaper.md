# rt_server Whitepaper

## Executive Summary

`rt_server` is a real-time pricing and risk-processing service implemented in Rust. It is designed to consume trade and market-related events, maintain current valuation state, compute portfolio-level pricing outputs, and publish downstream risk metrics over Kafka.

The system combines four primary design elements:

- asynchronous execution in Rust
- actor-based orchestration via `ractor`
- generic abstractions over markets and trades
- Kafka-based integration for inbound and outbound event flow

This architecture positions `rt_server` as an event-driven valuation engine suitable for low-latency internal pricing and risk workflows. Rather than relying on periodic batch recomputation, the platform is structured to react continuously to new trades, market updates, processor coordination messages, and post-processing events.

In its current form, the repository supports:
- real-time pricing across configurable pricing metrics
- portfolio publication keyed by pricing metric
- post-processing for aggregate NAV
- post-processing for a pragmatic delta-normal VaR estimate using diagonal covariance assumptions

The overall design is modular and extensible, with clear separation between market representation, trade behavior, pricing orchestration, and post-processing.

---

## 1. System Objectives

The purpose of `rt_server` is to provide a reusable runtime for streaming valuation and risk analytics. At a high level, the system is intended to:

1. ingest market and trade state from external systems
2. maintain in-memory representations of active markets and trades
3. price trades incrementally as new events arrive
4. aggregate trade-level outputs into portfolio-level results
5. publish those results for downstream consumers
6. support additional post-processors for derived portfolio analytics

This structure is well suited for environments where timeliness matters more than batch completeness, and where downstream systems require continuous updates rather than end-of-day reports.

---

## 2. Architectural Overview

The repository reflects a layered architecture with distinct runtime responsibilities.

### 2.1 Market abstraction

Markets are represented through the `MarketTypeT` trait. This trait provides a generic interface for market construction and access, allowing processors and pricing logic to operate without being tightly coupled to a single product implementation.

The broader repository includes multiple market implementations, including:
- AO markets
- LETF markets
- perpetual markets

A registry layer (`AllMarkets`) maintains currently available markets and processor-to-market associations. This provides a centralized mechanism for resolving the active market required by a pricing processor.

This abstraction is one of the major strengths of the system: it allows product-specific logic to remain local to market implementations while preserving common orchestration behavior at the processor layer.

### 2.2 Trade abstraction

Trades are represented by a small core trait, `BaseTrade`, which standardizes:
- trade identity through `id()`
- trade lifecycle direction through `direction()`

Pricing behavior is introduced through the `PriceTrade<MT>` trait, which defines:
- `initial_pv`
- `needs_recompute`
- `price`
- `pv01`
- `pnl`
- metric-based dispatch through `value_by_metric`

This split is appropriate. It keeps the base identity/lifecycle model separate from valuation behavior, while still allowing unified handling inside processors.

Trades are stored in `TradeRep<TR>`, a concurrent trade representation keyed by trade id. Because `TradeRep` itself implements pricing behavior when the contained trade type does, the system can treat a collection of trades as a priceable entity.

### 2.3 Processor architecture

The runtime uses actors for orchestration. In the currently provided files, `ProcessorCurr<T, MT>` is responsible for:
- maintaining current processor state
- reacting to trade and portfolio update messages
- pricing new trades on the active market
- updating portfolio state by pricing metric
- publishing portfolio outputs to Kafka

The processor is parameterized over both trade type and market type, reinforcing the generic architecture.

The current processor state tracks:
- active trades
- current portfolio results by metric
- current market
- enabled pricing metrics
- the number of new trades since the last market transition

This state model supports incremental recomputation rather than forcing a full repricing pass on every event.

### 2.4 Kafka integration

Kafka is a central integration boundary for `rt_server`.

The code provided shows two primary uses:
- consuming input messages through retry-enabled Kafka consumers
- publishing pricing and post-processing results through Kafka producers

In `processor_curr.rs`, published portfolio messages are keyed by `PricingMetric`, which now implements `rdkafka::message::ToBytes`. This gives the event stream a lightweight message discriminator at the Kafka key level, in addition to whatever semantic structure exists inside the payload.

Kafka is also used by the post-processing layer, discussed later in this document.

---

## 3. Pricing Model and Metrics

### 3.1 Supported pricing metrics

The current `PricingMetric` enum supports:
- `PV`
- `PV01`
- `PnL`

These metrics are serializable and also implement:
- `Display`
- `Hash`
- equality and copy semantics
- `ToBytes` for Kafka key publication

This is operationally useful because published pricing results can be distinguished by Kafka key without requiring full payload deserialization.

### 3.2 Metric-based valuation dispatch

The `PriceTrade<MT>` trait provides `value_by_metric`, which routes valuation behavior based on `PricingMetric`.

Current behavior:
- `PV` returns a single-trade portfolio containing the current price
- `PV01` returns a structured sensitivity result
- `PnL` computes current price minus initial PV and updates prior valuation state where relevant

This design is straightforward and extensible. New pricing metrics can be introduced by extending `PricingMetric` and implementing the corresponding branch in `value_by_metric`.

### 3.3 Portfolio publication

`ProcessorCurr::_publish_result_portfolio` serializes a `PortfolioType` and publishes it to Kafka with:
- topic: `results_topic`
- key: `PricingMetric`
- payload: serialized portfolio JSON

This publication model enables downstream post-processors to consume a unified stream of pricing outputs while preserving metric identity through the Kafka key.

---

## 4. Post-Processing Layer

A meaningful recent evolution of the repository is the emergence of a post-processing layer under `rt_server/src/postprocs/`.

This layer consumes pricing/risk outputs and publishes higher-level aggregate analytics.

### 4.1 NAV processor

`rt_server/src/postprocs/nav.rs` implements a `NavProcessor` that:
- consumes risk result messages from Kafka
- extracts the `PV` map
- maintains the latest known PV by trade id
- computes total NAV as the sum of all stored trade PVs
- publishes a `NavResultMessage` back to Kafka

The current result schema is:

- `id`
- `metric`
- `value`
- `kind`
- `ts_ms`

The processor is intentionally simple and acts as a reference implementation for Kafka-based post-processing.


### 4.3 VaR result schema

The VaR processor publishes `VarResultMessage` with:
- `id`
- `metric`
- `value`
- `kind`
- `ts_ms`
- `confidence`
- `methodology`

Current conventions are:
- `metric = "VaR"`
- `kind = "var"`
- `methodology = "delta_normal_diagonal"`

This is a strong foundation for downstream consumers because the result is self-describing and leaves room for future methodology upgrades.

### 4.4 Operational behavior

Both NAV and VaR processors follow the same operational pattern:
- create Kafka consumer and producer with retry-enabled connection helpers
- enter an infinite processing loop
- deserialize incoming messages
- update processor-local state
- compute an aggregate output
- publish the aggregate result
- log errors without terminating the process
- optionally restart via a wrapper loop

This consistency is valuable. It creates a reusable pattern for future post-processors such as stress, expected shortfall, concentration, or exposure summaries.

---

## 5. Data Contracts and Messaging Semantics

### 5.1 Portfolio stream semantics

Portfolio results published by current processors are emitted as serialized portfolio payloads keyed by `PricingMetric`. This allows downstream consumers to:
- process only relevant metrics
- branch behavior by key
- preserve topic consolidation when desired

### 5.2 Post-processing result semantics

Post-processed outputs such as NAV and VaR include explicit semantic fields in the payload:
- `metric`
- `kind`

This is useful because it supports payload-level distinction even when multiple result types share a Kafka topic.

### 5.3 Practical message discrimination

From an integration standpoint, the current repository now supports multiple ways to distinguish messages:
- topic
- key (`PricingMetric` for portfolio publication)
- payload discriminator (`kind`, `metric`)

This combination is operationally sound. It balances compatibility with Kafka partitioning semantics while preserving clear business meaning in the payload.

---

## 6. Strengths of the Current Design

### 6.1 Strong generic foundations

The market and trade traits are well chosen. They allow the runtime to support multiple product types without rewriting processor infrastructure.

### 6.2 Good fit for real-time workflows

The actor-based model, Kafka integration, and incremental processor state all align naturally with streaming pricing and risk use cases.

### 6.3 Clear extension point for derived analytics

The post-processing layer is now an explicit extension mechanism. NAV and VaR demonstrate how new aggregate analytics can be introduced without rewriting the core pricing engine.

### 6.4 Incremental rather than batch-oriented state updates

The current processor and post-processors both preserve internal state and update it incrementally. This is important for low-latency systems where recomputing the full world on every event would be too expensive.

### 6.5 Self-describing result payloads

The inclusion of fields such as `metric`, `kind`, `confidence`, and `methodology` in published results improves interoperability and future-proofs the message contracts.

---

## 7. Current Limitations and Engineering Considerations

### 7.1 VaR methodology is intentionally simplified

The current VaR processor uses a diagonal covariance model with fixed z-score buckets. This is appropriate as an initial real-time implementation, but it is not a full portfolio VaR framework.

Potential future enhancements include:
- full covariance or correlation support
- historical simulation
- filtered or regime-aware volatility inputs
- expected shortfall
- fallback hierarchy between delta-based and proxy PV-based risk

### 7.2 Some interfaces are still evolving

The repository still contains signs of active development, including unfinished branches such as `todo!()` in some areas. That does not undermine the design, but it does mean the implementation is still maturing.

### 7.3 Aggregation patterns are not yet fully unified

`nav.rs` and `var.rs` both define local aggregation traits and processor-specific aggregation logic. This is acceptable for now, but over time the repository may benefit from a more formal shared post-processing framework.

### 7.4 Kafka filtering remains consumer-side

Although messages can be differentiated by topic, key, and payload fields, Kafka does not provide general server-side filtering for ordinary consumers. This means downstream filtering still occurs at the consumer layer. The current message design supports this well, but it remains an operational consideration.

### 7.5 State ownership and cloning costs merit attention

Several parts of the code still rely on cloning for convenience during async iteration and pricing. This may be acceptable at current scale, but it is worth monitoring if trade counts or valuation frequency increase materially.

---

## 8. Recommended Evolution Path

A practical roadmap for the next stage of `rt_server` would include:

1. formalize message schemas across pricing and post-processing topics
2. standardize post-processor interfaces and lifecycle management
3. expand VaR from diagonal to correlated factor models
4. improve validation of incoming risk-factor inputs
5. reduce avoidable cloning in hot pricing paths
6. strengthen observability through structured tracing and processor-level metrics
7. document topic/key/payload conventions for all externally consumed streams

These steps would preserve the repository’s current strengths while improving operational clarity and analytical depth.

---

## 9. Conclusion

`rt_server` is best understood as a real-time, event-driven pricing and risk engine for streaming environments. Its architecture is built on sensible abstractions and increasingly clear separation of concerns:

- trade models define identity and pricing behavior
- market models define access to current state
- processors manage incremental valuation workflows
- Kafka provides integration and publication boundaries
- post-processors derive portfolio-level analytics such as NAV and VaR

The most notable recent architectural improvement is the emergence of the post-processing layer. With NAV and delta-normal VaR now represented as dedicated processors, the system has moved beyond pure pricing infrastructure and toward a broader real-time risk platform.

The current implementation is not yet a complete enterprise risk stack, nor does it claim to be. However, it already has the essential characteristics of a robust internal analytics service:
- modularity
- extensibility
- streaming-oriented design
- explicit message contracts
- a credible path toward richer portfolio risk analytics

In summary, the repository reflects a technically sound foundation for real-time pricing and risk computation, with a particularly strong trajectory toward extensible downstream analytics.
