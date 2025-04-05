use rdkafka::message::{BorrowedMessage, Message};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut, };
use tracing::{debug, warn};
use ractor::async_trait;
use std::fmt;

use crate::market::MarketType;
use crate::portfolio::{PV01Results, PortfolioType, PricingResults};
use crate::pricer::{Decoder, PriceTrade, PricingMetric};
use crate::process_trade::ProcessTradeValue;
use crate::ref_deref::{TryFromRef, TryFromRef2};
use crate::ref_deref_trait;
use crate::trade::TradeError;
use crate::trade::BaseTrade;
use crate::trade::TradeDirection;


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

impl PriceTrade for LETFTrade {
    fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    fn price(&self, market: &MarketType) -> Option<f64> {
        let stock_v = market.market.get(&self.stock);

        match stock_v {
            None => None,
            Some(stock_v_real) => self
                .stock_value
                .map(|initial_stock| self.beta * self.amount * (stock_v_real / initial_stock - 1.)),
        }
    }

    fn pv01(&self, market: &MarketType) -> PV01Results {
        let stock = market.market.get(&self.stock);

        match stock {
            None => {
                warn!("Could not obtain {:?} from the market", stock);
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

impl LETFTrade {
    /// produces the hedge of the LETF trade.
    /// stock_value : value of the stock that we are hedging LETF with.
    pub fn hedge(&mut self, market: &MarketType) -> Vec<LETFHedge> {
        let stock_name = &self.stock;
        let stock = match market.market.get(stock_name) {
	    None => {
		warn!(
                    "hedge: Could not find {:?} in the market. Leaving unhedged: {:?}",
                    stock_name, self,
		);
		return vec![]; // Cant do much w/ it.
            },
	    Some(sv) => *sv,
	};

        let trade_id = self.id();
        self.stock_value = Some(stock); // adding the actual value into the LETF  WEIRD
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

impl PriceTrade for Future {
    fn initial_pv(&self) -> Option<f64> {
        self.initial_val
    }

    fn price(&self, market: &MarketType) -> Option<f64> {
        let stock = market.market.get(&self.stock);
        debug!("_price: PriceTrade Future market: {:?}", market);

        stock.map(|stock_v| stock_v * self.amount)
    }

    fn pv01(&self, _market: &MarketType) -> PV01Results {
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
        self.trade_id.clone() // TODO: FIX THIS HERE!!!
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        //if self.amount >= 0. {
        TradeDirection::Create // TODO: THIS SHOULD OBVIOUSLY BE FIXED
                               //} else {
                               //    TradeDirection::Delete
                               //}
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

impl PriceTrade for Cash {
    fn initial_pv(&self) -> Option<f64> {
        Some(self.amount)
    }

    fn price(&self, _market: &MarketType) -> Option<f64> {
        Some(self.amount)
    }

    fn pv01(&self, _market: &MarketType) -> PV01Results {
        PV01Results::new()
    }
}

impl BaseTrade for Cash {
    fn id(&self) -> String {
        self.trade_id.clone() // TODO: THIS SHOULD BE FIXED
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        //if self.amount >= 0. {
        TradeDirection::Create // TODO: THIS SHOULD OBVIOUSLY BE FIXED
                               //} else {
                               //    TradeDirection::Delete
                               //}
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
}


impl fmt::Display for TradeTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pos_id = match self {
            TradeTypes::LETF(letf_trade) => &letf_trade.stock,
            TradeTypes::Future(letf_future) => &letf_future.stock,
            TradeTypes::Cash(cash) => &"cash".to_string(),
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
        }
    }

    #[allow(dead_code)]
    pub fn amount(&self) -> f64 {
        match self {
            TradeTypes::LETF(ref letf_trade) => letf_trade.amount,
            TradeTypes::Future(ref letf_future) => letf_future.amount,
            TradeTypes::Cash(letf_cash) => letf_cash.amount,
        }
    }
}

impl Decoder for TradeTypes {}
impl TryFromRef2 for TradeTypes {}


#[async_trait]
impl ProcessTradeValue for TradeTypes {
    async fn value_by_metric2(
        &self,
        metric: crate::pricer::PricingMetric,
        _pricing_options: &crate::pricer::MarketPricingOptions,
        curr_new_mkt: crate::market::MarketType,
    ) -> PricingResults  {
        let actual_market = curr_new_mkt;

        match metric {
            PricingMetric::PV => {
                let price = self.price(&actual_market);
                PricingResults::PV(PortfolioType::from([(self.id(), price.unwrap())]))
            }
            PricingMetric::PV01 => {
                let pv01 = self.pv01(&actual_market);
                PricingResults::PV01(pv01)
            }
            PricingMetric::PnL => {
                let pnl = self.pnl(&actual_market);
                PricingResults::PV(PortfolioType::from([(self.id(), pnl.unwrap_or(0.01))]))
            }
        }
    }
}

impl PriceTrade for TradeTypes {
    fn initial_pv(&self) -> Option<f64> {
        match self {
            TradeTypes::LETF(letf_trade) => letf_trade.initial_pv(),
            TradeTypes::Future(letf_fut) => letf_fut.initial_pv(),
            TradeTypes::Cash(letf_cash) => letf_cash.initial_pv(),
        }
    }

    fn price(&self, market: &MarketType) -> Option<f64> {
        match self {
            TradeTypes::LETF(letf_trade) => {
                match letf_trade.stock_value {
                    None => None,
                    Some(initial_value) => {
                        let mut letf_new = letf_trade.clone();
                        letf_new.stock_value = Some(initial_value); // TODO: HERE
                        letf_new.price(market)
                    }
                }
            }
            TradeTypes::Future(letf_fut) => letf_fut.price(market),
            TradeTypes::Cash(letf_cash) => letf_cash.price(market),
        }
    }

    fn pv01(&self, market: &MarketType) -> PV01Results {
        match self {
            TradeTypes::LETF(letf_trade) => {
                match letf_trade.stock_value {
                    None => PV01Results::new(), // TODO: HERE
                    Some(initial_value) => {
                        let mut letf_new = letf_trade.clone();
                        letf_new.stock_value = Some(initial_value); // TODO: HERE
                        letf_new.pv01(market)
                    }
                }
            }
            TradeTypes::Future(letf_fut) => letf_fut.pv01(market),
            TradeTypes::Cash(letf_cash) => letf_cash.pv01(market),
        }
    }
}

impl BaseTrade for TradeTypes {
    fn id(&self) -> String {
        match self {
            TradeTypes::LETF(letf_trade) => letf_trade.id(),
            TradeTypes::Future(letf_future) => letf_future.id(),
            TradeTypes::Cash(cash) => cash.id(),
        }
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        TradeDirection::Create // TODO: THIS SHOULD OBVIOUSLY BE FIXED
    }
}

pub type TradeTypesInner = TradeTypes;
pub struct TradeTypesRep(pub TradeTypesInner);

ref_deref_trait!(TradeTypesRep, TradeTypesInner);

impl BaseTrade for TradeTypesRep {
    fn id(&self) -> String {
        self.0.id()
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}

impl PriceTrade for TradeTypesRep {
    fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    fn price(&self, market: &MarketType) -> Option<f64> {
        self.0.price(market)
    }

    fn pv01(&self, market: &MarketType) -> PV01Results {
        self.0.pv01(market)
    }
}
