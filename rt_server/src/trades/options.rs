
use ractor::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tracing::{debug, warn};
use chrono::NaiveDateTime;


use crate::letf_market::{LETFMarketType, LETFMarketTypes};
use crate::market::MarketTypeT;
use crate::portfolio::{PV01Results, PortfolioType};
use crate::pricer::{Decoder, PriceTrade};
use crate::ref_deref::TryFromRef2;
use crate::trade::{BaseTrade, TradeDirection, TradeReduce};


pub struct StockOption {
    pub stock: String,
    pub amount: f64,
    pub strike: f64,
    pub expiry: NaiveDateTime,
}


#[async_trait]
impl PriceTrade<LETFMarketType> for StockOption {
    async fn initial_pv(&self) ...

}
