use log::{warn};
use core::cmp::Eq;
use serde_json::{Value,};
use serde::{Serialize, Deserialize,};
use kafka::consumer::Message;
use thiserror::Error;

use crate::ref_deref::TryFromRef;

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


#[derive(Error, Debug)]
pub enum TradeError {
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to trade type")]
    CantConvertToMarket(#[from] serde_json::Error),
}


impl<'a> TryFromRef<Message<'a>> for Trade
{
    type Error = TradeError;

    fn try_from_ref(value: &Message<'a>) -> Result<Self, Self::Error> {

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


//
// AO Trade here
//

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
