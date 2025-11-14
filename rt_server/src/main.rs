use all_markets::AllMarkets;
// Starts the controller.
use tracing::{info, Level, instrument};
use tracing_subscriber;
use tracing_subscriber::fmt::format::FmtSpan;
use trade_letf::{LETFHedge, TradeTypes};
use crate::pricer::PricingMetric;
use futures::future::join_all;
use axum::{
    routing::get,
    Router,
    extract::State
};
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use tokio::task;
use glommio::LocalExecutorBuilder;
use monoio;
use dotenv::dotenv;


//mod ao_risk;
//mod ao_risk_seq;
mod ao_trade;
//mod engine;
//mod letf_trader;
mod market;
mod portfolio;
mod portfolio_sender;
mod pricer;
//mod process_trade;
mod publish;
mod ref_deref;
//mod rm_local;
mod streaming;
mod trade;
//mod trader;
mod all_markets;

// actor framework new
pub mod trade_sender;
pub mod mkt_handler_actor;
pub(crate) mod processor_curr;
pub(crate) mod processor_new;
pub(crate) mod processor_bulk;
pub(crate) mod processor_middle;
pub(crate) mod engine_actor;
//pub(crate) mod engine_ao2;
pub(crate) mod processor_msg;
pub(crate) mod trade_letf;
pub(crate) mod ao_market;
pub(crate) mod engine_letf;
pub(crate) mod letf_market;

use crate::trade::TradeRep;
use crate::ao_market::AOMarketType;
//use crate::engine_ao2::start2;
use crate::engine_letf::start2;
use crate::letf_market::LETFMarketType;


// testing different
fn main_glommio() {
    let _ = LocalExecutorBuilder::default()
        .spawn(|| async move {
	    // here the async part
	    run_all().await;
	});
}


#[tokio::main]
async fn main() {
    run_all().await;
}

fn main_monoio() {

    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .build()
        .unwrap();

    rt.block_on(
	async {
            run_all().await
	}
    );
}

async fn run_all() {
    dotenv().ok();  // .env is loaded.

    let tracing_level = Level::INFO;
    tracing_subscriber::fmt()
        .with_max_level(tracing_level)
        //.with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .init();

    info!("Starting main system controller.");
    let host = std::env::var("HOST").expect("Could not find HOST in .env");
    let kafka_port = std::env::var("KAFKA_PORT")
        .expect("Could not find KAFKA_PORT in .env");
    let kafka_server = format!("{host}:{kafka_port}");
    let metric = PricingMetric::PV;
    let pos_topic = std::env::var("POSITIONS_TOPIC")
        .expect("Could not find POSITIONS_TOPIC in .env");  // "air_options.ao.option_positions"
    let mkt_topic = std::env::var("MKT_TOPIC")
        .expect("Could not find MKT_TOPIC in .env");  // "air_options.ao.mkt_events"
    let results_topic = std::env::var("RESULTS_TOPIC")
        .expect("Could not find RESULTS_TOPIC in .env");  //"air_options.ao.results"
    let market_port = std::env::var("MARKET_PORT")
        .expect("Could not find MARKET_PORT in .env");
    let pricing_port = std::env::var("PRICING_PORT")
        .expect("Could not find PRICING_PORT in .env");

    let kafka_params = engine_actor::KafkaParams {
	kafka_server,
	pos_topic,
	mkt_topic,
	results_topic,
    };
    
    let state = Arc::new(
        Mutex::new(portfolio::PortfolioType::default())
    );
    let state2 = state.clone();

    let axum_process = task::spawn(
	async move {
	    let app = Router::new()
		.route("/portfolio", get(portfolio_handler))
		.with_state(state2);

	    //let listener = tokio::net::TcpListener::bind("192.168.1.51:3000").await.unwrap();

	    info!("Starting axum");
	    let addr: SocketAddr = format!("{host}:3000").parse().unwrap();
	    //axum::serve(listener, app).await.unwrap();
	    axum_server::bind(addr).serve(app.into_make_service())
                .await
                .unwrap();
        }
    );

    let mut results = vec![axum_process];
    let markets_used = vec!["curr".to_string(), "new".to_string()];
    
    let (initial_trades, all_markets) = init_letf();

    let mut all_actors = start2(
    	kafka_params,
        metric,
        all_markets,
        markets_used,
        initial_trades,
    ).await;

    results.append(&mut all_actors);
    // tokio::join!(results);
    join_all(results).await;
}

// initialize the letf market.
fn init_letf() -> (Arc<TradeRep<TradeTypes>>, Arc<AllMarkets<LETFMarketType>>) {
    let initial_trades = Arc::new(TradeRep::<TradeTypes>::default());  // defines the type of trades.
    let initial_market = LETFMarketType::new("name1".to_string());  // TODO: CHANGE HERW
    let all_markets = Arc::new(AllMarkets::<Arc<LETFMarketType>>::new());  // how many in-between markets there are.
 
    (initial_trades, all_markets)
}


fn init_ao() {
    todo!()
}


#[instrument]
async fn portfolio_handler(
    State(state): State<Arc<Mutex<portfolio::PortfolioType>>>
) -> String {
    let s = (*(state.lock().unwrap())).clone();
    let s_disp = format!("LEN = {:?} PORTFOLIO = {:?}", s.len(), s);
    s_disp
}
