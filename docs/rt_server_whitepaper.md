# rt_server Whitepaper

## Overview

`rt_server` is a real-time pricing and risk-processing service built around async Rust, actor-based orchestration, Kafka messaging, and generic abstractions for trades and markets. Based on the shared files, its main role is to receive market and trade updates, maintain current state, distribute pricing work across processors, and publish valuation-related outputs.

The design emphasizes streaming workflows over batch workflows. Instead of treating pricing as a periodic offline calculation, `rt_server` appears designed to continuously react to new trades, market updates, and setup/configuration messages.

At a high level, the system combines:

- Kafka consumers/producers for external integration
- actor-based execution using `ractor`
- generic trade and market traits
- concurrent state containers for trades and markets
- processor layers that propagate updates through the system

This gives `rt_server` the shape of an event-driven pricing engine suitable for low-latency internal risk infrastructure.

## Architecture Summary

The core architecture seems to revolve around four building blocks.

### 1. Trade abstraction

The `BaseTrade` trait defines the minimal common behavior every trade must expose:

- a stable identifier via `id()`
- lifecycle intent via `direction()`

Trades are stored in `TradeRep<TR>`, which is a concurrent trade container keyed by trade id. This allows the engine to manage many trade instances while remaining generic over trade type.

The `extend_trade!` macro in `trade.rs` adds a practical extension mechanism: it can wrap a trade-like structure into a new structure that includes `prev_pv: Option<f64>`. This is useful for incremental PnL or revaluation workflows where the engine needs both the current valuation and the previous one.

### 2. Market abstraction

Markets are represented behind the `MarketTypeT` trait. The broader repository shows implementations for LETF, perp, and AO market types. This abstraction allows processors and pricing logic to interact with a common market interface while preserving product-specific implementation details.

A separate `AllMarkets` registry appears to maintain the active market set and processor-to-market mappings, giving the engine a centralized view of currently relevant market state.

### 3. Actor orchestration

The runtime is coordinated using actors. In `engine_actor.rs`, the code constructs current and middle processors and links them into chains. This suggests a staged processing model where updates flow through multiple actors, potentially for filtering, aggregation, recomputation control, or progressive valuation.

In `processor_setup_actor.rs`, a dedicated actor listens for setup messages from Kafka and distributes pricing metric updates to processor actors. This is a clean operational pattern: control-plane messages are separated from core market/trade flow.

### 4. Kafka integration

Kafka appears to be the primary integration mechanism for both input and output. The engine listens for setup and likely trade/position updates, while publishing results outward. This makes `rt_server` fit naturally inside a larger distributed trading or risk platform.

## Main Advantages

### 1. Strong extensibility

The trade and market abstractions are a major strength. Adding new product types does not require redesigning the whole runtime; instead, new types can implement the common traits and integrate into existing processor flows. This is valuable in financial systems where product coverage evolves over time.

### 2. Good alignment with real-time processing

The architecture is well matched to streaming use cases:

- async execution
- actor isolation
- Kafka messaging
- concurrent state storage

Together these choices support reactive pricing and risk updates rather than slow batch recomputation.

### 3. Clear separation of responsibilities

The code separates concerns reasonably well:

- trades define identity and behavior
- markets define state access
- processors coordinate computation
- setup actors handle control-plane updates
- Kafka adapters manage external connectivity

This separation should help the codebase scale as functionality grows.

### 4. Practical support for stateful valuation

The trade-extension macro that adds `prev_pv` is a useful pattern. It enables the engine to attach incremental valuation state to existing trades without rewriting every trade model. That is especially helpful for PnL tracking, change detection, and recomputation optimization.

## Main Disadvantages

### 1. Generic infrastructure still contains product-specific assumptions

The new trade-extension macro currently includes a `PriceTrade<LETFMarketType>` implementation directly in the macro expansion. That makes the mechanism less generic than it appears. If the same extension is needed for non-LETF products, the current approach may create duplication or force awkward coupling.

In short, the architecture wants to be generic, but some implementation choices still leak product-specific logic into shared infrastructure.

### 2. Operational flow may become complex to reason about

Actor chains are powerful, but they can also make behavior harder to trace, especially when combined with asynchronous messaging and shared concurrent state. As the number of processors and message types grows, observability and debugging become more difficult unless logging, tracing, and message semantics are very disciplined.

### 3. Setup handling is simple but somewhat rigid

The setup actor reads one message, forwards it, then waits for the next. This is workable, but the pattern may become limiting if setup traffic grows more varied or if processors need acknowledgements, versioning, partial rollout, or error recovery semantics.

### 4. Some parts of the broader repository remain unfinished

From the read-only summaries, several modules in the repository still contain `todo!()` calls, temporary comments, and hardcoded values. That does not invalidate the architecture, but it does mean the runtime’s conceptual strengths are stronger than its current implementation maturity in some areas.

## Conclusion

`rt_server` is best understood as a modular real-time pricing/risk engine built for an event-driven environment. Its major strengths are:

- extensible trade and market abstractions
- a runtime model suited to streaming updates
- actor-based orchestration
- clean integration with Kafka
- support for incremental valuation state such as `prev_pv`

Its main weaknesses are:

- some leakage of product-specific logic into generic layers
- rising complexity from async actor chains
- relatively rigid setup/control handling
- incomplete implementation maturity in parts of the broader codebase

Overall, the system has a solid architectural direction. Its advantages are strongest at the design level: flexibility, composability, and alignment with real-time workflows. Its disadvantages are mainly around maintainability and generalization, which are fixable with careful refactoring and stronger operational discipline.
