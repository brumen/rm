// #![warn(clippy::large_futures)]
//#![feature(async_fn_in_trait)]
// #![feature(impl_trait_in_assoc_type)]
pub mod market;
pub mod portfolio;
pub mod portfolio_sender;
pub mod pricer;
// pub mod process_trade;
pub mod publish;
pub mod ref_deref;
pub mod streaming;
pub mod trade;
// pub mod trader;

pub(crate) mod all_markets;
pub(crate) mod engine_actor;
pub(crate) mod engine_letf;
pub(crate) mod markets;
pub(crate) mod mkt_handler_actor;
pub(crate) mod processor_bulk;
pub(crate) mod processor_curr;
pub(crate) mod processor_middle;
pub(crate) mod processor_msg;
pub(crate) mod processor_new;
pub(crate) mod trade_sender;
pub(crate) mod trades;
// pub(crate) mod processor_setup;  // TODO: include this after fixing the axum crate.
pub(crate) mod processor_setup_actor;
pub(crate) mod utils;
