// Starts the controller.

use tokio;
use tracing_subscriber;

mod ao_risk;
mod ao_trade;
mod controller;
mod encdec;
mod engine;
mod letf_trader;
mod market;
mod mkt_handler;
mod portfolio;
mod portfolio_sender;
mod pricer;
mod publish;
mod ref_deref;
mod rm_local;
mod streaming;
mod trade;
mod trade_procs;
mod trader;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();

    // letf_trader::main_letf_trader();
    ao_risk::ao_main_risk().await;
}
