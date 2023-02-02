use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum TradeDirection {
    Create,
    Delete,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct Trade {
    pub trade_id: u8,
    pub direction: TradeDirection,
}

pub trait TradeHandling {
    fn recover_trade(msg_decoded: &Value) -> Trade;
}
