use log::{warn, debug,};
use core::cmp::Eq;
use serde_json::Value;
use serde::{Serialize, Deserialize,};
use kafka::consumer::Message;
use thiserror::Error;
use uuid::Uuid;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut, };
 
use crate::portfolio::{PV01Results, PortfolioType};
use crate::ref_deref::TryFromRef;
use crate::pricer::PriceTrade;
use crate::market::MarketType;

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub enum TradeDirection {
    Create,
    Delete,
    Update,
}


pub trait BaseTrade {
    fn id(&self) -> String;
    fn direction(&self) -> TradeDirection;
}


#[derive(Error, Debug)]
pub enum TradeError {
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to trade type")]
    CantConvertToTrade(#[from] serde_json::Error),
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LETFTrade {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
    pub beta: f64,
}

impl BaseTrade for LETFTrade {
    fn id(&self) -> String {
        self.trade_id.clone()  // TODO: FIX THIS LATER - MAYBE JUST A REF!!!
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
    
    fn price(&self, market: &MarketType) -> Option<f64> {
        let stock = market.get(&self.stock);
        debug!("_price: Market = {:?}", market);
        stock.map(|stock_v| self.amount )
    }

    fn pv01(&self, market: &MarketType) -> PV01Results {
        let mut pv01_result = PV01Results::new();

	let stock = market.get(&self.stock);

	match stock {
	    None => {
		warn!("Could not obtain {:?} from the market", stock);
	    },
	    Some(stock_v) => {
		let _ = pv01_result.insert(self.trade_id.clone(), PortfolioType::from([(self.stock.clone(), self.beta * self.amount / stock_v),]));		
	    },
	}

        pv01_result
    }
}

impl LETFTrade {

    /// produces the hedge of the LETF trade.
    /// stock_value : value of the stock that we are hedging LETF with.
    pub fn hedge(&self, market: &MarketType) -> Vec<LETFHedge> {

        let stock_name = &self.stock;
        let stock_value = market.get(stock_name);

        if stock_value.is_none() {
            warn!("hedge: Could not find {:?} in the market", stock_name);
            return vec![]; // Cant do much w/ it.
        }

        let stock = stock_value.unwrap();
        let beta = self.beta;
        let amount = self.amount;

        vec![
            LETFHedge::Future( Future {
                trade_id: Uuid::new_v4().to_string(),
                stock: stock_name.clone(),
                amount: - beta * amount / stock,
            }),
            LETFHedge::Cash( Cash {
                trade_id: Uuid::new_v4().to_string(),
                amount : (beta - 1.) * amount
            }),
        ]
    }
}


impl TryFromRef<Message<'_>> for LETFTrade
{
    type Error = TradeError;

    fn try_from_ref(value: &Message) -> Result<Self, Self::Error> {

        let msg_utf = std::str::from_utf8(value.value)?;
        debug!("__try_from_ref: {:?}", msg_utf);

        Ok(serde_json::from_str::<LETFTrade>(msg_utf)?)
    }
}


impl TryFromRef<Message<'_>> for TradeTypes {
    type Error = TradeError;

    fn try_from_ref(value: &Message) -> Result<Self, Self::Error> {

        let msg_utf = std::str::from_utf8(value.value)?;

        Ok(serde_json::from_str::<TradeTypes>(msg_utf)?)
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Future {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
}

impl PriceTrade for Future {
    fn price(&self, market: &MarketType) -> Option<f64> {

        let stock = market.get(&self.stock);
	debug!("_price: PriceTrade Future market: {:?}", market);
	
        stock.map(|stock_v| stock_v * self.amount)
    }

    fn pv01(&self, _market: &MarketType) -> PV01Results {

        let mut pv01_results = PV01Results::new();
        let _ = pv01_results.insert(self.trade_id.clone(), PortfolioType::from([(self.stock.clone(), self.amount),]));

	debug!("PriceTrade: pv01 Future: {:?}", pv01_results);
        pv01_results
    }
}

impl BaseTrade for Future {
    fn id(&self) -> String {
        self.trade_id.clone()  // TODO: FIX THIS HERE!!!
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        //if self.amount >= 0. {
        TradeDirection::Create  // TODO: THIS SHOULD OBVIOUSLY BE FIXED
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
    fn price(&self, _market: &MarketType) -> Option<f64> {
        Some(self.amount)
    }

    fn pv01(&self, _market: &MarketType) -> PV01Results {
        PV01Results::new()
    }
}

impl BaseTrade for Cash {
    fn id(&self) -> String {
        self.trade_id.clone()  // TODO: THIS SHOULD BE FIXED
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        //if self.amount >= 0. {
        TradeDirection::Create  // TODO: THIS SHOULD OBVIOUSLY BE FIXED
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


impl TradeTypes {

    pub fn trade_name(&self) -> String {
        match self {
            TradeTypes::LETF(ref letf_trade) => letf_trade.stock.clone(),
            TradeTypes::Future(ref letf_future) => letf_future.stock.clone(),
            TradeTypes::Cash(_) => "Cash".to_string(),
        }
    }

    pub fn amount(&self) -> f64 {
        match self {
            TradeTypes::LETF(ref letf_trade) => letf_trade.amount,
            TradeTypes::Future(ref letf_future) => letf_future.amount,
            TradeTypes::Cash(letf_cash) => letf_cash.amount,
        }
    }
}

impl PriceTrade for TradeTypes {
    fn price(&self, market: &MarketType) -> Option<f64> {
        match self {
            TradeTypes::LETF(letf_trade) => letf_trade.price(market),
            TradeTypes::Future(letf_fut) => letf_fut.price(market),
            TradeTypes::Cash(letf_cash) => letf_cash.price(market),
        }
    }

    fn pv01(&self, market: &MarketType) -> PV01Results {
        match self {
            TradeTypes::LETF(letf_trade) => letf_trade.pv01(market),
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
        TradeDirection::Create  // TODO: THIS SHOULD OBVIOUSLY BE FIXED
    }
}


pub trait LETFTradeHandling {
    fn recover_trade(msg_decoded: &Value) -> Option<LETFTrade>;
}


//
// AO Trade here
//


//         // trade is not None, continue w/ this.
//         let msg_payload = &msg_decoded["payload"];
//         let event_type = &msg_payload["op"];

//         debug!("Getting position: {:?}", msg_payload);

//         match event_type.as_str() {
//             Some("c") => {
//                 let tid = msg_payload["after"]["position_id"].as_i64();
//                 return Some(Trade {
//                     trade_id: tid.unwrap() as u16,
//                     direction: TradeDirection::Create,
//                 });
//             }
//             Some("d") => {
//                 let tid = msg_payload["before"]["position_id"].as_i64();
//                 return Some(Trade {
//                     trade_id: tid.unwrap() as u16,
//                     direction: TradeDirection::Delete,
//                 });
//             }
//             _ => {
//                 warn!("UNIMPLEMENTED. THIS SHOULD NOT HAPPEN. EXAMINE. ");
//                 return Some(Trade {
//                     trade_id: 189,
//                     direction: TradeDirection::Create,
//                 });
//             }
//         }
//     }
// }


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct Payload {
    op: String,
    after: AfterPosition,
    before: BeforePosition,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct AfterPosition {
    position_id: i64,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct BeforePosition {
    position_id: i64,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct AOTrade {
    payload: Payload,
}


impl BaseTrade for AOTrade {
    fn id(&self) -> String {
        self.payload.after.position_id.to_string()
    }
    fn direction(&self) -> TradeDirection {
        match self.payload.op.as_str() {
            "c" => TradeDirection::Create,
            "d" => TradeDirection::Delete,
            &_ => todo!(),
        }
    }
}


impl TryFromRef<Message<'_>> for AOTrade
{
    type Error = TradeError;

    fn try_from_ref(value: &Message) -> Result<Self, Self::Error> {

        let msg_utf = std::str::from_utf8(value.value)?;

        Ok(serde_json::from_str::<AOTrade>(msg_utf)?)
    }
}


/// Internal representations of trades.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeRep<TT> (
    pub HashMap<String, TT>
);

impl<TT> Deref for TradeRep<TT> {
    type Target = HashMap::<String, TT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<TT> DerefMut for TradeRep<TT> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


impl<TT> TradeRep<TT>
where
    TT: BaseTrade + PartialEq
{
    pub fn new() -> Self {
	    TradeRep(HashMap::<String, TT>::new())
    }

    pub fn add_trade(&mut self, trade: TT) {
	    let trade_id = trade.id();

	    // TODO: FIX THIS PART BELOW.
	    let trade_position = self.keys().position(|tradeid| tradeid.eq(&trade_id));

	    if trade_position.is_none() {
	        self.insert(trade_id, trade);
	    }
    }

    pub fn get(&self, trade_id: &String) -> Option<TT> {
	    self.get(trade_id)
    }

    pub fn all_trade_names(&self) -> Vec<String> {
        self.all_trades_ref()
            .into_iter()
            .map(|t| t.id())
            .collect()
    }

    pub fn all_trades_ref(&self) -> Vec<&TT> {
	self.values().into_iter().collect::<Vec<&TT>>()
    }

    pub fn contains(&self, trade: &TT) -> bool {
	self.all_trades_ref().contains(&trade)
    }
}


