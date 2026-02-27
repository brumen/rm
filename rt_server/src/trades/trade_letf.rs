use ractor::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::market::MarketTypeT;
use crate::markets::letf_market::{LETFMarketType, LETFMarketTypes};
use crate::portfolio::{PV01Results, PortfolioType};
use crate::pricer::{Decoder, HedgeTrade, PriceTrade};
use crate::ref_deref::TryFromRef2;
use crate::trade::{BaseTrade, TradeDirection, TradeReduce};
use crate::trades::perp::PerpTrade;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LETFTrade {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
    pub beta: f64,
    pub stock_value: Option<f64>,
}

impl fmt::Display for LETFTrade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pos_id = &self.trade_id;
        write!(f, "{}", pos_id)
    }
}

impl BaseTrade for LETFTrade {
    fn id(&self) -> String {
        self.trade_id.clone() // TODO: FIX THIS LATER - MAYBE JUST A REF!!!
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        if self.amount < 0. {
            TradeDirection::Delete
        } else {
            TradeDirection::Create
        }
    }
}

impl std::cmp::PartialEq for LETFTrade {
    fn eq(&self, other: &Self) -> bool {
        self.trade_id == other.trade_id
    }
}

#[async_trait]
impl PriceTrade<LETFMarketType> for LETFTrade {
    async fn needs_recompute(
        &self,
        market_old: Arc<LETFMarketType>,
        market_new: Arc<LETFMarketType>,
    ) -> bool {
        let stock_v_real_old = market_old
            .get(&LETFMarketTypes::Stock(self.stock.clone())) // TODO: CHECK IF WE DONT NEED TO CLONE HERE!!!
            .await;
        let stock_v_real_new = market_new
            .get(&LETFMarketTypes::Stock(self.stock.clone()))
            .await;

        stock_v_real_new != stock_v_real_old
    }

    async fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    async fn price(&self, market: Arc<LETFMarketType>) -> Option<f64> {
        let stock_v_real = market
            .get(&LETFMarketTypes::Stock(self.stock.clone())) // TODO: CHECK IF WE DONT NEED TO CLONE HERE!!!
            .await?;

        self.stock_value
            .map(|initial_stock| self.beta * self.amount * (stock_v_real / initial_stock - 1.))
    }

    async fn pv01(&self, market: Arc<LETFMarketType>) -> PV01Results {
        let stock = market
            .get(&LETFMarketTypes::Stock(self.stock.clone()))
            .await;

        match stock {
            None => {
                warn!("Could not obtain stock {:?} from the market.", self.stock);
                PV01Results::new()
            }
            Some(_stock_v) => match self.stock_value {
                None => {
                    warn!("pv01: LETFTrade: could not find the initial stock value");
                    PV01Results::new()
                }
                Some(initial_stock) => {
                    let mut pv01_result = PV01Results::new();
                    let _ = pv01_result.insert(
                        self.trade_id.clone(),
                        PortfolioType::from([(
                            self.stock.clone(),
                            self.beta * self.amount / initial_stock,
                        )]),
                    );
                    debug!("_pv01: LETF trade: {:?}", pv01_result);
                    pv01_result
                }
            },
        }
    }
}

#[async_trait]
impl HedgeTrade<LETFMarketType, Vec<LETFHedge>> for LETFTrade {
    async fn hedge(&self, market: LETFMarketType) -> Vec<LETFHedge> {
        let stock_name = &self.stock;
        let stock = match market
            .get(&LETFMarketTypes::Stock(stock_name.clone()))
            .await
        {
            None => {
                warn!(
                    "hedge: Could not find {:?} in the market. Leaving unhedged: {:?}",
                    stock_name, self,
                );
                return vec![]; // Cant do much w/ it.
            }
            Some(sv) => sv,
        };

        let trade_id = self.id();
        // self.stock_value = Some(stock); // adding the actual value into the LETF  WEIRD
        let beta = self.beta;
        let amount = self.amount;

        // Computing the hedge.
        vec![
            LETFHedge::Future(Future {
                initial_val: Some(-beta * amount),
                trade_id: (trade_id.parse::<i32>().unwrap() + 1).to_string(), //Uuid::new_v4().to_string(),
                stock: stock_name.clone(),
                amount: -beta * amount / stock,
            }),
            LETFHedge::Cash(Cash {
                trade_id: (trade_id.parse::<i32>().unwrap() + 2).to_string(), // Uuid::new_v4().to_string(),
                amount: beta * amount,
            }),
        ]
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Future {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
    pub initial_val: Option<f64>,
}

#[async_trait]
impl PriceTrade<LETFMarketType> for Future {
    async fn needs_recompute(
        &self,
        market_old: Arc<LETFMarketType>,
        market_new: Arc<LETFMarketType>,
    ) -> bool {
        let stock_v_real_old = market_old
            .get(&LETFMarketTypes::Stock(self.stock.clone())) // TODO: CHECK IF WE DONT NEED TO CLONE HERE!!!
            .await;
        let stock_v_real_new = market_new
            .get(&LETFMarketTypes::Stock(self.stock.clone()))
            .await;

        stock_v_real_new != stock_v_real_old
    }

    async fn initial_pv(&self) -> Option<f64> {
        self.initial_val
    }

    async fn price(&self, market: Arc<LETFMarketType>) -> Option<f64> {
        let stock = market
            .get(&LETFMarketTypes::Stock(self.stock.clone()))
            .await?;

        Some(stock * self.amount)
    }

    async fn pv01(&self, _market: Arc<LETFMarketType>) -> PV01Results {
        let mut pv01_results = PV01Results::new();
        let _ = pv01_results.insert(
            self.trade_id.clone(),
            PortfolioType::from([(self.stock.clone(), self.amount)]),
        );
        debug!("_pv01: PriceTrade: pv01 Future: {:?}", pv01_results);

        pv01_results
    }
}

impl BaseTrade for Future {
    fn id(&self) -> String {
        self.trade_id.clone()
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}

impl std::cmp::PartialEq for Future {
    fn eq(&self, other: &Self) -> bool {
        self.trade_id == other.trade_id
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cash {
    pub trade_id: String,
    pub amount: f64,
}

#[async_trait]
impl PriceTrade<LETFMarketType> for Cash {
    async fn needs_recompute(
        &self,
        market_old: Arc<LETFMarketType>,
        market_new: Arc<LETFMarketType>,
    ) -> bool {
        false
    }

    async fn initial_pv(&self) -> Option<f64> {
        Some(self.amount)
    }

    async fn price(&self, _market: Arc<LETFMarketType>) -> Option<f64> {
        Some(self.amount)
    }

    async fn pv01(&self, _market: Arc<LETFMarketType>) -> PV01Results {
        PV01Results::new()
    }
}

impl BaseTrade for Cash {
    fn id(&self) -> String {
        self.trade_id.clone() // TODO: THIS SHOULD BE FIXED
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}

// TODO: MAKE A MACRO FOR THIS!!!
impl std::cmp::PartialEq for Cash {
    fn eq(&self, other: &Self) -> bool {
        self.trade_id == other.trade_id
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LETFHedge {
    Future(Future),
    Cash(Cash),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TradeTypes {
    LETF(LETFTrade),
    Future(Future),
    Cash(Cash),
    Perp(PerpTrade),
}

impl fmt::Display for TradeTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pos_id: &str = match self {
            TradeTypes::LETF(letf_trade) => letf_trade.stock.as_str(),
            TradeTypes::Future(letf_future) => letf_future.stock.as_str(),
            TradeTypes::Cash(_cash) => "cash",
            TradeTypes::Perp(perp) => perp.underlying.as_str(),
        };
        write!(f, "{}", pos_id)
    }
}

impl TradeTypes {
    #[allow(dead_code)]
    pub fn trade_name(&self) -> String {
        match self {
            TradeTypes::LETF(ref letf_trade) => letf_trade.stock.clone(),
            TradeTypes::Future(ref letf_future) => letf_future.stock.clone(),
            TradeTypes::Cash(_) => "Cash".to_string(),
            TradeTypes::Perp(ref perp) => perp.underlying.clone(),
        }
    }

    #[allow(dead_code)]
    pub fn amount(&self) -> f64 {
        match self {
            TradeTypes::LETF(ref letf_trade) => letf_trade.amount,
            TradeTypes::Future(ref letf_future) => letf_future.amount,
            TradeTypes::Cash(letf_cash) => letf_cash.amount,
            TradeTypes::Perp(perp) => perp.amount,
        }
    }
}

impl Decoder for TradeTypes {}
impl TryFromRef2 for TradeTypes {}

#[async_trait]
impl PriceTrade<LETFMarketType> for TradeTypes {
    async fn needs_recompute(
        &self,
        market_old: Arc<LETFMarketType>,
        market_new: Arc<LETFMarketType>,
    ) -> bool {
        match self {
            TradeTypes::LETF(letf_trade) => {
                letf_trade
                    .needs_recompute(market_old.clone(), market_new.clone())
                    .await
            }
            TradeTypes::Future(letf_fut) => {
                letf_fut
                    .needs_recompute(market_old.clone(), market_new.clone())
                    .await
            }
            TradeTypes::Cash(letf_cash) => {
                letf_cash
                    .needs_recompute(market_old.clone(), market_new.clone())
                    .await
            }
            TradeTypes::Perp(perp_trade) => {
                perp_trade
                    .needs_recompute(market_old.clone(), market_new.clone())
                    .await
            }
        }
    }

    async fn initial_pv(&self) -> Option<f64> {
        match self {
            TradeTypes::LETF(letf_trade) => letf_trade.initial_pv().await,
            TradeTypes::Future(letf_fut) => letf_fut.initial_pv().await,
            TradeTypes::Cash(letf_cash) => letf_cash.initial_pv().await,
            TradeTypes::Perp(perp_trade) => perp_trade.initial_pv().await,
        }
    }

    async fn price(&self, market: Arc<LETFMarketType>) -> Option<f64> {
        match self {
            TradeTypes::LETF(letf_trade) => match letf_trade.stock_value {
                None => None,
                Some(initial_value) => {
                    let mut letf_new = letf_trade.clone();
                    letf_new.stock_value = Some(initial_value); // TODO: CHECK HERE
                    letf_new.price(market).await
                }
            },
            TradeTypes::Cash(letf_cash) => letf_cash.price(market).await,
            TradeTypes::Future(letf_fut) => letf_fut.price(market).await,
            TradeTypes::Perp(perp_trade) => perp_trade.price(market).await,
        }
    }

    async fn pv01(&self, market: Arc<LETFMarketType>) -> PV01Results {
        match self {
            TradeTypes::LETF(letf_trade) => match letf_trade.stock_value {
                None => PV01Results::new(), // TODO: HERE
                Some(initial_value) => {
                    let mut letf_new = letf_trade.clone();
                    letf_new.stock_value = Some(initial_value); // TODO: HERE
                    letf_new.pv01(market).await
                }
            },
            TradeTypes::Future(letf_fut) => letf_fut.pv01(market).await,
            TradeTypes::Cash(letf_cash) => letf_cash.pv01(market).await,
            TradeTypes::Perp(perp_trade) => perp_trade.pv01(market).await,
        }
    }
}

impl BaseTrade for TradeTypes {
    fn id(&self) -> String {
        match self {
            TradeTypes::LETF(letf_trade) => letf_trade.id(),
            TradeTypes::Future(letf_future) => letf_future.id(),
            TradeTypes::Cash(cash) => cash.id(),
            TradeTypes::Perp(perp_trade) => perp_trade.id(),
        }
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        match self {
            TradeTypes::LETF(letf_trade) => letf_trade.direction(),
            TradeTypes::Future(letf_future) => letf_future.direction(),
            TradeTypes::Cash(cash) => cash.direction(),
            TradeTypes::Perp(perp_trade) => perp_trade.direction(),
        }
    }
}

impl TradeReduce for TradeTypes {
    type TradeType = TradeTypes;
    type ReductionType = TradeTypes;

    fn reduce(&self, trade: &Self::TradeType) -> Self::ReductionType {
        trade.clone() // TODO: FIX THIS LATER, WITHOUT CLONE
    }
}
