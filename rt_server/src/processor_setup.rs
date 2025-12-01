// Setup server for the processor.
use axum::{
    extract::State,
    routing::{get, post},
    Router,
    Json,
};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::task::{self, JoinHandle};
use tracing::info;
use serde::Deserialize;

use crate::portfolio;
use crate::pricer::PricingMetric;


#[derive(Deserialize)]
struct SetupRequest {
    metric: String,
}

pub(crate) fn axum_process(host: String) -> JoinHandle<()> {
    let state = Arc::new(Mutex::new(portfolio::PortfolioType::default()));
    let state2 = state.clone();

    let axum_process = task::spawn(async move {
        let app = Router::new()
            .route("/portfolio", get(portfolio_handler))
            .route("/setup", post(setup_handler))
            .with_state(state2);

        //let listener = tokio::net::TcpListener::bind("192.168.1.51:3000").await.unwrap();

        let addr: SocketAddr = format!("{host}:3000").parse().unwrap();
        info!("Starting setup process on {:?}", addr);
        axum_server::bind(addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    axum_process
}

async fn portfolio_handler(State(state): State<Arc<Mutex<portfolio::PortfolioType>>>) -> String {
    let s = (*(state.lock().unwrap())).clone();
    let s_disp = format!("LEN = {:?} PORTFOLIO = {:?}", s.len(), s);
    s_disp
}

async fn setup_handler(
    State(state): State<Arc<Mutex<portfolio::PortfolioType>>>,
    Json(payload): Json<SetupRequest>,
) -> String {
    let metric_str = payload.metric.to_uppercase();
    let metric = match metric_str.as_str() {
        "PV" => PricingMetric::PV,
        "PV01" => PricingMetric::PV01,
        "PNL" => PricingMetric::PnL,
        _ => return format!("Invalid metric: {}", payload.metric),
    };

    let _s = state.lock().unwrap();
    format!("Setup complete with metric: {:?}", metric)
}
