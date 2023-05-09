use core::cmp::Eq;
use serde_json::{Value,};
use serde::{Serialize, Deserialize,};


#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum TradeDirection {
    Create,
    Delete,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct Trade {
    pub trade_id: u16,
    pub direction: TradeDirection,
}

pub trait AOTradeHandling {
    fn recover_trade_ao(msg_decoded: &Value) -> Option<Trade>;
}


///
/// encoding the LETF trade.
#[derive(Debug, Serialize, Deserialize)]
pub struct LETFTrade {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
    pub beta: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LETFFuture {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LETFCash {
    pub trade_id: String,
    pub amount: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LETFHedge {
    Future(LETFFuture),
    Cash(LETFCash),
}

pub trait LETFTradeHandling {
    fn recover_trade(msg_decoded: &Value) -> Option<LETFTrade>;
}
