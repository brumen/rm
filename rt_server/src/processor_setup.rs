// Setup server for the processor.
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::task::{self, JoinHandle};
use tracing::info;

use crate::portfolio;

pub(crate) fn axum_process(host: String) -> JoinHandle<()> {
    let state = Arc::new(Mutex::new(portfolio::PortfolioType::default()));
    let state2 = state.clone();

    let axum_process = task::spawn(async move {
        let app = Router::new()
            .route("/portfolio", get(portfolio_handler))
            .with_state(state2);

        let addr: SocketAddr = format!("{host}:3000").parse().unwrap();
        info!("Starting axum process serving on {:?}", addr);
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
