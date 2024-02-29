// Starts the controller.
use tracing::Level;


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
mod process_trade;

#[tokio::main]
async fn main() {

    // console_subscriber::init();

    let tracing_level = Level::INFO;
    tracing_subscriber::fmt()
        .with_max_level(tracing_level)
        .init();

    letf_trader::main_letf_trader().await;
    //ao_risk::ao_main_risk().await;
}
