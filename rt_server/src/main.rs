// Starts the controller.
use crate::pricer::PricingMetric;
use dotenv::dotenv;
use futures::future::join_all;
use std::sync::Arc;
use tracing::{info, Level};
use trade_letf::TradeTypes;

mod all_markets;
mod market;
mod portfolio;
mod portfolio_sender;
mod pricer;
mod publish;
mod ref_deref;
mod streaming;
mod trade;

// actor framework new
pub(crate) mod ao_market;
pub(crate) mod engine_actor;
pub(crate) mod engine_letf;
pub(crate) mod letf_market;
pub(crate) mod mkt_handler_actor;
pub(crate) mod processor_bulk;
pub(crate) mod processor_curr;
pub(crate) mod processor_middle;
pub(crate) mod processor_msg;
pub(crate) mod processor_new;
pub(crate) mod processor_setup;
pub(crate) mod trade_letf;
pub(crate) mod trade_sender;
pub(crate) mod utils;

use crate::engine_letf::start2;
use crate::letf_market::LETFMarketType;
use crate::trade::TradeRep;

#[tokio::main]
async fn main() {
    run_all().await;
}

async fn run_all() {
    info!("Reading data from .env");
    dotenv().ok(); // .env is loaded.
    let host = std::env::var("HOST").expect("Could not find HOST in .env");
    let kafka_port = std::env::var("KAFKA_PORT").expect("Could not find KAFKA_PORT in .env");
    let kafka_server = format!("{host}:{kafka_port}");
    let metric = PricingMetric::PV;
    let pos_topic =
        std::env::var("POSITIONS_TOPIC").expect("Could not find POSITIONS_TOPIC in .env"); // "air_options.ao.option_positions"
    let mkt_topic = std::env::var("MKT_TOPIC").expect("Could not find MKT_TOPIC in .env"); // "air_options.ao.mkt_events"
    let results_topic =
        std::env::var("RESULTS_TOPIC").expect("Could not find RESULTS_TOPIC in .env"); //"air_options.ao.results"
    let market_port = std::env::var("MARKET_PORT").expect("Could not find MARKET_PORT in .env");
    let pricing_port = std::env::var("PRICING_PORT").expect("Could not find PRICING_PORT in .env");
    info!(".env data loaded.");
    let kafka_params = engine_actor::KafkaParams {
        kafka_server,
        pos_topic,
        mkt_topic,
        results_topic,
    };

    let tracing_level = Level::INFO;
    tracing_subscriber::fmt()
        .with_max_level(tracing_level)
        //.with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .init();

    info!("Starting setup actor.");
    let axum_process = processor_setup::axum_process(host);

    let mut all_handles = vec![axum_process];
    let markets_used = vec!["curr".to_string(), "new".to_string()];

    let (initial_trades, all_markets) = init_letf();

    info!("Starting main system controller.");
    let mut all_actors = start2(
        kafka_params,
        metric,
        all_markets,
        markets_used,
        initial_trades,
        (),
        2,
    )
    .await;

    all_handles.append(&mut all_actors);
    // tokio::join!(results);
    join_all(all_handles).await;
}

// initialize the letf market.
fn init_letf() -> (
    Arc<TradeRep<TradeTypes>>,
    Arc<all_markets::AllMarkets<Arc<LETFMarketType>>>,
) {
    let initial_trades = Arc::new(TradeRep::<TradeTypes>::default()); // defines the type of trades.
    let all_markets = Arc::new(all_markets::AllMarkets::<Arc<LETFMarketType>>::new()); // how many in-between markets there are.

    (initial_trades, all_markets)
}
