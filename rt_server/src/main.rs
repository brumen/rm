// Starts the controller.
use tracing::{info, Level, instrument};
use tracing_subscriber;
use tracing_subscriber::fmt::format::FmtSpan;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::engine_actor::start2;
use futures::future::join_all;
use axum::{
    routing::get,
    Router,
    extract::State
};
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use tokio::task;

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



#[tokio::main]
async fn main() {

    let tracing_level = Level::INFO;
    tracing_subscriber::fmt()
        .with_max_level(tracing_level)
        //.with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .init();

    info!("Starting main system controller.");
    let kafka_server = "192.168.1.107:9092".to_string();
    let metric = PricingMetric::PV;
    let pos_topic = "air_options.ao.option_positions".to_string();
    let mkt_topic = "air_options.ao.mkt_events".to_string();
    let results_topic = "air_options.ao.results".to_string();
    // let mkt_params = MktMsgParams::AOParams();
    let pricing_options = MarketPricingOptions {
	pricing_server: "192.168.1.107:8000".to_string(),
	pricing_endpoint: "pv".to_string(),
    };

    let state = Arc::new(Mutex::new(portfolio::PortfolioType::default()));
    let state2 = state.clone();
    
    let axum_process = task::spawn(
	async move {
	    let app = Router::new()
		.route("/portfolio", get(portfolio_handler))
		.with_state(state2);
	    
	    //let listener = tokio::net::TcpListener::bind("192.168.1.51:3000").await.unwrap();

	    info!("Starting axum");
	    let addr: SocketAddr = "192.168.1.51:3000".parse().unwrap();
	    //axum::serve(listener, app).await.unwrap();
	    axum_server::bind(addr).serve(app.into_make_service())
                .await
                .unwrap();
        }
    );

    let mut results = vec![axum_process];
    
    let (processor_curr, mut result) = start2(
    	kafka_server, metric, pos_topic, mkt_topic, results_topic, &pricing_options, state,
    ).await;

    results.append(&mut result);
    // tokio::join!(results);
    //axum_process.await.unwrap();
    // result.insert(0, axum_process);
    join_all(results).await;
}


#[instrument]
async fn portfolio_handler(
    State(state): State<Arc<Mutex<portfolio::PortfolioType>>>
) -> String {
    let s = (*(state.lock().unwrap())).clone();
    let s_disp = format!("LEN = {:?} PORTFOLIO = {:?}", s.len(), s);
    s_disp
}
