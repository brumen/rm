use axum::{
    extract::{Query, State},
    routing::get,
    Router,
};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::task::{self, JoinHandle};
use tracing::info;

use crate::all_markets;
use crate::markets::letf_market::LETFMarketType;
use crate::portfolio;
use crate::pricer::PriceTrade;
use crate::trade::TradeRep;
use crate::trades::trade_letf::TradeTypes;

type PortfolioState = Arc<Mutex<portfolio::PortfolioType>>;
type MarketsState = Arc<all_markets::AllMarkets<Arc<LETFMarketType>>>;
type Trades = Arc<TradeRep<TradeTypes>>;

#[derive(Clone)]
struct DiagnosticsState {
    portfolio: PortfolioState,
    all_markets: MarketsState,
    all_trades: Trades,
}

#[derive(serde::Deserialize)]
struct PriceQuery {
    market: String,
    trade_id: String,
}

pub(crate) fn diagnostics(
    host: String,
    all_markets: MarketsState,
    initial_trades: Trades,
) -> JoinHandle<()> {
    let state = DiagnosticsState {
        portfolio: Arc::new(Mutex::new(portfolio::PortfolioType::default())),
        all_markets,
        all_trades: initial_trades,
    };

    let axum_process = task::spawn(async move {
        let app = Router::new()
            .route("/portfolio", get(portfolio_handler))
            .route("/market", get(market_handler))
            .route("/market_map", get(market_map_handler))
            .route("/trades", get(trades_handler))
            .route("/price", get(price_handler))
            .with_state(state);

        let addr = format!("{host}:3000");
        info!("Starting diagnostics process serving on {}", addr);

        let listener = TcpListener::bind(&addr).await.unwrap();
        axum::serve(listener, app.into_make_service())
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
    format!("Markets: {:?}", market_names)
}

async fn market_map_handler(State(state): State<DiagnosticsState>) -> String {
    let market_map_names = state.all_markets.list_processor_names();
    format!("Market map: {:?}", market_map_names)
}

async fn trades_handler(State(state): State<DiagnosticsState>) -> String {
    let trades = state.all_trades;
    format!("Trades: {:?}", trades)
}

// /price?market=23232&trade_id=123
async fn price_handler(
    State(state): State<DiagnosticsState>,
    Query(params): Query<PriceQuery>,
) -> String {
    let market_name = params.market;
    let trade_id = params.trade_id;

    let market = match state.all_markets.get(&market_name) {
        None => {
            return format!(
                "ERROR: market '{}' not found. Available markets: {:?}",
                market_name,
                state.all_markets.list_market_names()
            );
        }
        Some(m) => m,
    };

    let trade_entry = state.all_trades.get(&trade_id);
    let trade_entry = match trade_entry {
        None => {
            return format!(
                "ERROR: trade_id '{}' not found. Known trades: {:?}",
                trade_id,
                state.all_trades.all_trade_names()
            );
        }
        Some(t) => t,
    };

    let trade = trade_entry.value();

    match trade.price(market).await {
        None => format!(
            "ERROR: price returned None for trade_id='{}' market='{}'",
            trade_id, market_name
        ),
        Some(px) => format!(
            "OK: trade_id='{}' market='{}' price={}",
            trade_id, market_name, px
        ),
    }
}
