use log::debug;
use core::cmp::Eq;
use serde_json::Value;
use serde::{Serialize, Deserialize,};
use kafka::consumer::Message;
use thiserror::Error;
use uuid::Uuid;

use crate::ref_deref::TryFromRef;

#[allow(dead_code)]
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

pub trait BaseTrade {
    fn id(&self) -> u16;
    fn direction(&self) -> TradeDirection;
}

impl BaseTrade for Trade {
    fn id(&self) -> u16 {
        self.trade_id
    }
    fn direction(&self) -> TradeDirection {
        self.direction
    }
}


#[derive(Error, Debug)]
pub enum TradeError {
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to trade type")]
    CantConvertToTrade(#[from] serde_json::Error),
}


impl TryFromRef<Message<'_>> for Trade
{
    type Error = TradeError;

    fn try_from_ref(value: &Message) -> Result<Self, Self::Error> {

        let msg_utf = std::str::from_utf8(value.value)?;

        // TODO: THIS IS OF COURSE WRONG!!!
        let message_json = serde_json::from_str::<AOTrade>(msg_utf)?;

        Ok(
            Self {
                trade_id: message_json.id(),
                direction: message_json.direction(),
            }
        )
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LETFTrade {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
    pub beta: f64,
}

impl BaseTrade for LETFTrade {
    fn id(&self) -> u16 {
        100  // TODO: OBVIOUSLY FIX HERE
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


impl LETFTrade {

    /// pricing the LETFTrade
    pub fn price(&self, stock_value : f64) -> f64 {
        // TODO: FIX THIS VALUE TO BE MORE REFLECTIVE.
        stock_value
    }

    /// produces the hedge of the LETF trade.
    /// stock_value : value of the stock that we are hedging LETF with.
    pub fn hedge(&self, stock_value : f64) -> Vec<LETFHedge> {
        let beta = self.beta;
        let amount = self.amount;
        let exposure_amt = beta * amount * stock_value;

        vec![
            LETFHedge::Future( Future {
                trade_id: Uuid::new_v4().to_string(),
                stock: self.stock.clone(), //  stock_name,
                amount: exposure_amt,
            }),
            LETFHedge::Cash( Cash {
                trade_id: Uuid::new_v4().to_string(),
                amount : - exposure_amt
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
        debug!("__try_from_ref: {:?}", msg_utf);

        Ok(serde_json::from_str::<TradeTypes>(msg_utf)?)
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Future {
    pub trade_id: String,
    pub stock: String,
    pub amount: f64,
}

impl BaseTrade for Future {
    fn id(&self) -> u16 {
        100  // TODO: FIX THIS TO INFER IT FROM actual trade id
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        if self.amount >= 0. {
            TradeDirection::Create  // TODO: THIS SHOULD OBVIOUSLY BE FIXED
        } else {
            TradeDirection::Delete
        }
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

impl BaseTrade for Cash {
    fn id(&self) -> u16 {
        100  // TODO: FIX THIS TO INFER IT FROM actual trade id
    }

    // TODO: THIS SHOULD BE FIXED.
    fn direction(&self) -> TradeDirection {
        if self.amount >= 0. {
            TradeDirection::Create  // TODO: THIS SHOULD OBVIOUSLY BE FIXED
        } else {
            TradeDirection::Delete
        }
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


impl BaseTrade for TradeTypes {
    fn id(&self) -> u16 {
        match self {
            TradeTypes::LETF(letf_trade) => 100, // TODO: OBVIOUSLY THIS IS WRONG
            TradeTypes::Future(letf_future) => 100,
            TradeTypes::Cash(cash) => 100,
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


#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    op: String,
    after: AfterPosition,
    before: BeforePosition,
}


#[derive(Debug, Serialize, Deserialize)]
struct AfterPosition {
    position_id: i64,
}


#[derive(Debug, Serialize, Deserialize)]
struct BeforePosition {
    position_id: i64,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct AOTrade {
    payload: Payload,
}


impl BaseTrade for AOTrade {
    fn id(&self) -> u16 {
        self.payload.after.position_id as u16
    }
    fn direction(&self) -> TradeDirection {
        match self.payload.op.as_str() {
            "c" => TradeDirection::Create,
            "d" => TradeDirection::Delete,
            &_ => todo!(),
        }
    }
}
