use axum::{
    extract::State,
    routing::get,
    Router,
};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::task::{self, JoinHandle};
use tracing::info;

use crate::all_markets;
use crate::markets::letf_market::LETFMarketType;
use crate::portfolio;

type PortfolioState = Arc<Mutex<portfolio::PortfolioType>>;
type MarketsState = Arc<all_markets::AllMarkets<Arc<LETFMarketType>>>;

#[derive(Clone)]
struct DiagnosticsState {
    portfolio: PortfolioState,
    all_markets: MarketsState,
}

pub(crate) fn diagnostics(host: String, all_markets: MarketsState) -> JoinHandle<()> {
    let state = DiagnosticsState {
        portfolio: Arc::new(Mutex::new(portfolio::PortfolioType::default())),
        all_markets,
    };

    let axum_process = task::spawn(async move {
        let app = Router::new()
            .route("/portfolio", get(portfolio_handler))
            .route("/market", get(market_handler))
            .with_state(state);

        let addr: SocketAddr = format!("{host}:3000").parse().unwrap();
        info!("Starting diagnostics process serving on {:?}", addr);
        axum_server::bind(addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    axum_process
}

async fn portfolio_handler(State(state): State<DiagnosticsState>) -> String {
    let s = (*(state.portfolio.lock().unwrap())).clone();
    format!("LEN = {:?} PORTFOLIO = {:?}", s.len(), s)
}

async fn market_handler(State(state): State<DiagnosticsState>) -> String {
    let market_names = state.all_markets.list_market_names();
    format!("LEN = {} MARKETS = {:?}", market_names.len(), market_names)
}
