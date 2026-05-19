// Starts the controller.
use dotenv::dotenv;
use futures::future::join_all;
use local_ip_address::local_ip;
use std::sync::Arc;
use tracing::{error, info, Level};
use tracing_subscriber::{layer::SubscriberExt, reload, util::SubscriberInitExt, EnvFilter};

pub mod market;
pub mod portfolio;
pub mod portfolio_sender;
mod postprocs;
pub mod pricer;
pub mod publish;
pub mod ref_deref;
pub mod streaming;
pub mod trade;

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
pub(crate) mod processor_setup;
pub(crate) mod processor_setup_actor;
pub(crate) mod trade_sender;
pub(crate) mod trades;
pub(crate) mod utils;

use crate::engine_letf::start2;
use crate::markets::letf_market::LETFMarketType;
use crate::postprocs::{nav, var};
use crate::processor_bulk::PricingStyle;
use crate::processor_curr::RTOperatingMode;
use crate::processor_setup_actor::start_setup_actor;
use crate::trade::TradeRep;
use trades::trade_letf::TradeTypes;

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
    let pos_topic =
        std::env::var("POSITIONS_TOPIC").expect("Could not find POSITIONS_TOPIC in .env"); // "air_options.ao.option_positions"
    let mkt_topic = std::env::var("MKT_RAW_TOPIC").expect("Could not find MKT_RAW_TOPIC in .env"); // "air_options.ao.mkt_events"
    let results_topic =
        std::env::var("RESULTS_TOPIC").expect("Could not find RESULTS_TOPIC in .env"); //"air_options.ao.results"
    let setup_topic = std::env::var("SETUP_TOPIC").expect("Could not find SETUP_TOPIC in .env");
    let _market_port = std::env::var("MARKET_PORT").expect("Could not find MARKET_PORT in .env");
    let _pricing_port = std::env::var("PRICING_PORT").expect("Could not find PRICING_PORT in .env");
    let debug_level = std::env::var("DEBUG_LEVEL").expect("Could not find DEBUG in .env");
    let pricing_mode = match std::env::var("PRICING_MODE")
        .expect("Could not find PRICING_MODE in .env")
        .as_str()
    {
        "sequential" => PricingStyle::Sequential,
        "parallel_one_thread" => PricingStyle::ParallelSingleThread,
        _ => PricingStyle::Parallel,
    };
    let operating_mode = match std::env::var("OPERATING_MODE")
        .expect("Could not find OPERATING_MODE in .env")
        .as_str()
    {
        "single" => RTOperatingMode::SingleBuffer,
        _ => RTOperatingMode::DoubleBuffer,
    };
    // number of middle processors.
    let nb_middle_procs = match operating_mode {
        RTOperatingMode::SingleBuffer => 0,
        _ => 1,
    };
    let kafka_params = engine_actor::KafkaParams {
        kafka_server: kafka_server.clone(),
        pos_topic: pos_topic.clone(),
        mkt_topic: mkt_topic.clone(),
        results_topic: results_topic.clone(),
    };

    let kafka_params2 = engine_actor::KafkaParams {
        kafka_server: kafka_server.clone(),
        pos_topic,
        mkt_topic: mkt_topic.clone(),
        results_topic: "letf.risk2".to_string(),
    };

    let tracing_level = match debug_level.as_str() {
        "info" => Level::INFO,
        "debug" => Level::DEBUG,
        _ => Level::INFO,
    };

    // Reloadable log filter layer
    let initial_filter = EnvFilter::from_default_env().add_directive(tracing_level.into());
    let (_reload_layer, reload_handle) = reload::Layer::new(initial_filter);

    tracing_subscriber::fmt()
        .compact() // Focuses on the current span/target
        .with_target(true)
        .init();

    // this below is enabled if you want to reload and all.
    //    tracing_subscriber::registry()
    //        .with(tracing_subscriber::fmt::layer())
    //        .with(reload_layer)
    //        .init();

    let mut all_handles = vec![];
    let markets_used = vec!["curr".to_string(), "new".to_string()];

    let (initial_trades, all_markets) = init_letf();

    let Ok(local_ip_address) = local_ip() else {
        error!("Error getting local IP addres.");
        return;
    };

    info!("Starting main system controller.");
    let (all_actors, mut all_actors_handles, state_distr_new) = start2(
        kafka_params,
        all_markets.clone(),
        markets_used.clone(),
        initial_trades.clone(),
        (),
        nb_middle_procs,
        operating_mode.clone(),
        pricing_mode.clone(),
    )
    .await;

    info!("Starting main system controller.");
    let (all_actors2, mut all_actors_handles2, _state_distr_new2) = start2(
        kafka_params2,
        all_markets.clone(),
        markets_used,
        initial_trades.clone(),
        (),
        0, // 0 middle processors (TODO: CHECK THIS)
        RTOperatingMode::SingleBuffer,
        pricing_mode,
    )
    .await;

    let diagnostics_handle = processor_setup::diagnostics(
        local_ip_address.to_string(),
        all_markets,
        initial_trades,
        reload_handle,
        state_distr_new.clone(),
    );
    all_handles.push(diagnostics_handle);

    // postprocessing handles
    let mut total_nav_processor = nav::NavProcessor::new(&kafka_server, &results_topic, "")
        .expect("Could not start NAV processor");
    let total_nav = tokio::spawn(async move {
        total_nav_processor.run_ignore().await;
    });

    let mut total_var_processor = var::VarProcessor::new_with_market_config(
        &kafka_server,
        &results_topic,
        "",
        0.95,
        &mkt_topic,
        250,
        20,
    )
    .expect("Could not start VaR processor");

    let total_var = tokio::spawn(async move {
        total_var_processor.run_ignore().await;
    });

    // this creates the setup actor.
    let setup_actor_handle = start_setup_actor(host.clone(), setup_topic.clone(), all_actors).await;
    let setup_actor_handle2 =
        start_setup_actor(host.clone(), setup_topic.clone(), all_actors2).await;

    info!("All relevant actors initialized.");
    all_handles.append(&mut all_actors_handles);
    all_handles.push(setup_actor_handle);
    // post processing handles
    all_handles.push(total_nav);
    all_handles.push(total_var);
    // kafka_params2 setup.
    all_handles.append(&mut all_actors_handles2);
    all_handles.push(setup_actor_handle2);

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
