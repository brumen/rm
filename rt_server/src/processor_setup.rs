use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
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
use crate::trade::TradeRep;
use crate::trades::trade_letf::TradeTypes;

type PortfolioState = Arc<Mutex<portfolio::PortfolioType>>;
type MarketsState = Arc<all_markets::AllMarkets<Arc<LETFMarketType>>>;
type Trades = Arc<TradeRep<TradeTypes>>;
type ReloadHandle = reload::Handle<EnvFilter, Layered<Layer<Registry>, Registry>>;
type LayerFilterMap = HashMap<String, String>;

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
    /// Example: "debug"
    level: String,

    /// Optional target/module filter, e.g.: "rt_server::processor_bulk"
    /// If omitted, applies globally.
    target: Option<String>,

    /// Optional additional directives, comma-separated.
    /// Example: "hyper=warn,tower_http=info"
    directives: Option<String>,

    /// Optional span events (tracing-subscriber fmt layer), e.g.:
    ///   "none"|"new"|"enter"|"exit"|"close"|"active"|"full"
    ///
    /// Note: this is acknowledged but not applied with the current EnvFilter reload handle.
    span_events: Option<String>,
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
            .route("/loglevel", post(loglevel_handler))
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
    let market_names = state.all_markets.list_market_names().await;
    format!("Markets: {:?}", market_names)
}

async fn market_map_handler(State(state): State<DiagnosticsState>) -> String {
    let market_map_names = state.all_markets.list_processor_names().await;
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

    let market = match state.all_markets.get(&market_name).await {
        None => {
            return format!(
                "ERROR: market '{}' not found. Available markets: {:?}",
                market_name,
                state.all_markets.list_market_names().await
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

async fn loglevel_handler(
    State(state): State<DiagnosticsState>,
    Json(params): Json<HashMap<String, String>>,
) -> String {
    // POST /loglevel with JSON dictionary body, e.g.:
    //   {"level":"debug"}
    //   {"level":"debug","target":"rt_server::processor_bulk"}
    //   {"level":"debug","directives":"hyper=warn,tower_http=info"}
    //   {"level":"debug","target":"rt_server","directives":"hyper=warn"}
    //
    // Note: `span_events` can't be applied via the EnvFilter reload handle.
    info!("Reconfiguring logs.");

    let level = match params.get("level") {
        Some(v) => v.to_lowercase(),
        None => return "ERROR: missing required key 'level'".to_string(),
    };

    let directive_level = match level.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => level.as_str(),
        _ => "ERROR: invalid level. Use one of: trace, debug, info, warn, error",
    };
    if directive_level.starts_with("ERROR:") {
        return directive_level.to_string();
    }

    let target = params
        .get("target")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let mut directive_strs: Vec<String> = vec![];

    // Base directive from (target, level)
    directive_strs.push(match target.as_deref() {
        Some(t) => format!("{}={}", t, directive_level),
        None => directive_level.to_string(),
    });

    // Optional extra directives, comma-separated
    if let Some(extra) = params.get("directives") {
        for part in extra.split(',') {
            let part = part.trim();
            if !part.is_empty() {
                directive_strs.push(part.to_string());
            }
        }
    }

    // Optional layer filters:
    // Keys: layer_1, layer_2, ... values: filter expressions compatible with EnvFilter directives.
    // Example payload:
    //   {"level":"debug","layer_1":"rt_server::processor_bulk=trace","layer_2":"hyper=warn"}
    //
    // NOTE: With the current reload handle type, we can only reload the global EnvFilter, not swap
    // out/add additional filtered layers. We *can* combine all directives into one EnvFilter, which
    // provides equivalent "target/field/span" filtering semantics at the subscriber level.
    //
    // If you want true per-layer filters (different filters per output layer), we need to change
    // the subscriber wiring to use multiple layers with `layer.with_filter(...)` and hold reload
    // handles per layer.
    let mut layer_filters: LayerFilterMap = HashMap::new();
    for (k, v) in &params {
        if let Some((_prefix, rest)) = k.split_once("layer_") {
            if !rest.trim().is_empty() && !v.trim().is_empty() {
                layer_filters.insert(k.clone(), v.trim().to_string());
            }
        }
    }

    // Build a single EnvFilter containing all directives:
    // - base directive from level/target
    // - optional extra directives
    // - optional layer_* directives (merged)
    let mut all_directives = directive_strs.clone();
    if !layer_filters.is_empty() {
        // deterministic order for nicer debugging/response
        let mut keys: Vec<_> = layer_filters.keys().cloned().collect();
        keys.sort();
        for lk in keys {
            if let Some(expr) = layer_filters.get(&lk) {
                all_directives.push(expr.clone());
            }
        }
    }

    let mut new_filter = EnvFilter::from_default_env();
    for ds in &all_directives {
        let parsed = match ds.parse() {
            Ok(d) => d,
            Err(e) => {
                return format!(
                    "ERROR: invalid directive '{}': {}. Examples: {{\"level\":\"debug\"}} or {{\"level\":\"debug\",\"target\":\"rt_server::processor_bulk\"}} or {{\"level\":\"debug\",\"directives\":\"hyper=warn,tower_http=info\"}} or {{\"level\":\"debug\",\"layer_1\":\"rt_server::processor_bulk=trace\"}}",
                    ds, e
                );
            }
        };
        new_filter = new_filter.add_directive(parsed);
    }

    let reload_res = state.reload_handle.reload(new_filter);

    let span_events_note =
        match params.get("span_events").map(|s| s.trim()).filter(|s| !s.is_empty()) {
            None => None,
            Some(se) => Some(format!(
                "NOTE: span_events='{}' requested but not applied (not supported by current reload handle).",
                se
            )),
        };

    match reload_res {
        Ok(()) => {
            let mut notes: Vec<String> = vec![];

            if let Some(note) = span_events_note {
                notes.push(note);
            }

            if !layer_filters.is_empty() {
                notes.push(format!(
                    "NOTE: layer_* directives were merged into the global filter (per-layer filters require subscriber changes). Received: {:?}",
                    layer_filters
                ));
            }

            if notes.is_empty() {
                format!("OK: log directives set to {:?}", all_directives)
            } else {
                format!(
                    "OK: log directives set to {:?}. {}",
                    all_directives,
                    notes.join(" ")
                )
            }
        }
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
