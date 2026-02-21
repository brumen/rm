use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::task::{self, JoinHandle};
use tracing::info;
use tracing_subscriber::{fmt::Layer, layer::Layered, reload, EnvFilter, Registry};

use crate::all_markets;
use crate::markets::letf_market::LETFMarketType;
use crate::portfolio;
use crate::pricer::PriceTrade;
use crate::processor_msg::PNStateDistr;
use crate::processor_msg::ProcessorMiddleMessageStates;
use crate::trade::TradeRep;
use crate::trades::trade_letf::TradeTypes;

type PortfolioState = Arc<Mutex<portfolio::PortfolioType>>;
type MarketsState = Arc<all_markets::AllMarkets<Arc<LETFMarketType>>>;
type Trades = Arc<TradeRep<TradeTypes>>;
type ReloadHandle = reload::Handle<EnvFilter, Layered<Layer<Registry>, Registry>>;

#[derive(Clone)]
struct DiagnosticsState {
    portfolio: PortfolioState,
    all_markets: MarketsState,
    all_trades: Trades,
    reload_handle: ReloadHandle,
    state_distr_new: Arc<PNStateDistr>,
}

#[derive(serde::Deserialize)]
struct PriceQuery {
    market: String,
    trade_id: String,
}

#[derive(serde::Deserialize)]
struct LogLevelQuery {
    level: String,
}

pub(crate) fn diagnostics(
    host: String,
    all_markets: MarketsState,
    initial_trades: Trades,
    reload_handle: ReloadHandle,
    state_distr_new: Arc<PNStateDistr>,
) -> JoinHandle<()> {
    let state = DiagnosticsState {
        portfolio: Arc::new(Mutex::new(portfolio::PortfolioType::default())),
        all_markets,
        all_trades: initial_trades,
        reload_handle,
        state_distr_new,
    };

    let axum_process = task::spawn(async move {
        let app = Router::new()
            .route("/portfolio", get(portfolio_handler))
            .route("/market", get(market_handler))
            .route("/market_map", get(market_map_handler))
            .route("/trades", get(trades_handler))
            .route("/price", get(price_handler))
            .route("/loglevel", get(loglevel_handler))
            .route("/state_distr_new", get(state_distr_new_handler))
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

    let trade_entry = state.all_trades.read_sync(&trade_id, |_, v| v.clone());
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

    match trade_entry.price(market).await {
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

// /loglevel?level=debug
async fn loglevel_handler(
    State(state): State<DiagnosticsState>,
    Query(params): Query<LogLevelQuery>,
) -> String {
    let level = params.level.to_lowercase();

    let directive = match level.as_str() {
        "trace" => "trace",
        "debug" => "debug",
        "info" => "info",
        "warn" => "warn",
        "error" => "error",
        _ => {
            return "ERROR: invalid level. Use one of: trace, debug, info, warn, error".to_string();
        }
    };

    let new_filter = EnvFilter::from_default_env().add_directive(directive.parse().unwrap());

    match state.reload_handle.reload(new_filter) {
        Ok(()) => format!("OK: log level set to {}", directive),
        Err(e) => format!("ERROR: failed to reload log filter: {}", e),
    }
}

// displays the state distribution
async fn state_distr_new_handler(
    State(state): State<DiagnosticsState>,
) -> Json<HashMap<String, u64>> {
    let mut snapshot: HashMap<String, u64> = HashMap::new();

    for entry in state.state_distr_new.iter() {
        let ek = *entry.key();
        snapshot.insert(ek.to_string(), *entry.value());
    }

    Json(snapshot)
}
