//! The incoming processor is not lead-throttled, allowing it to catch up.
//! Admission and pricing concurrency remain bounded.
//!
//! `engine_letf::start2` constructs the shared `Processor` with `Role::New`.
//! Its message handling and market-refresh decisions are in `processor_flow`;
//! `pump` launches pricing, `PricingDone` applies outcomes, and `flush_output`
//! forwards the resulting portfolio to the next stage.
#[allow(unused_imports)]
pub(crate) use crate::processor_flow::Processor as ProcessorNew;
