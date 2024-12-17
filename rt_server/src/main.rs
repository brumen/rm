// Starts the controller.
use tracing::Level;

//mod ao_risk;
//mod ao_risk_seq;
mod ao_trade;
//mod controller;
//mod controller_seq;
//mod encdec;
//mod engine;
//mod letf_trader;
mod market;
//mod mkt_handler;
mod portfolio;
mod portfolio_sender;
mod pricer;
mod process_trade;
mod publish;
mod ref_deref;
//mod rm_local;
mod streaming;
mod trade;
//mod trade_procs;
//mod trader;

// actor framework new
pub mod trade_sender;
pub mod mkt_handler_actor;
pub mod processor_curr;
pub mod processor_new;
pub mod processor_bulk;
pub mod engine_actor;


use crate::market::MktMsgParams;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::engine_actor::start2;
use futures::future::join_all;


#[tokio::main]
async fn main() {

    let metric = PricingMetric::PV;
    let pos_topic = "positions".to_string();
    let mkt_topic = "market".to_string();
    let results_topic = "results".to_string();
    let mkt_params = MktMsgParams::AOParams();
    let pricing_options = MarketPricingOptions {
	pricing_server: "localhost".to_string(),
	pricing_endpoint: "pv".to_string(),
    };

    
    let result = start2(
	metric, pos_topic, mkt_topic, results_topic, mkt_params, &pricing_options,
    ).await;
    join_all(result).await;
}
