use log::warn;
use serde::{Serialize, Deserialize,};
use kafka::consumer::Message;
use std::collections::HashMap;

use crate::portfolio::{PV01Results, PortfolioType};
use crate::ref_deref::TryFromRef;
use crate::pricer::{Decoder, PricingMetric, PriceTradeAsync, MarketPricingOptions,};
use crate::portfolio::PricingResults;
use crate::market::{MarketType, CurrNewMarket,};
use crate::trade::BaseTrade;
use crate::trade::{TradeDirection, TradeError,};

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
struct _Payload {
    op: String,
    after: _AfterPosition,
    before: _BeforePosition,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct _AfterPosition {
    position_id: i64,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
struct _BeforePosition {
    position_id: i64,
}


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct AOTrade {
    payload: _Payload,
}


impl Decoder for AOTrade {

    // converts the spark response into a trade value.
    fn _unwrap_pricing_results(
        &self,
        result_price: reqwest::blocking::Response,
        metric: PricingMetric,
    ) -> PricingResults {

        match metric {
            PricingMetric::PV => {
                let results_conv = result_price.json::<HashMap<String, f64>>();

                if results_conv.is_err() {
                    return PricingResults::PV(PortfolioType::new())
                }

                PricingResults::PV(PortfolioType(results_conv.unwrap()))
            },
            PricingMetric::PV01 => {
                let results_conv = result_price.json::<HashMap<String, HashMap<String, f64>>>();

                if results_conv.is_err() {
                    return PricingResults::PV01(PV01Results::new());
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert((*trade_id.clone()).to_string(), PortfolioType::from(trade_result));
                }
                PricingResults::PV01(pv01)
            },
	    PricingMetric::PnL => todo!(),
        }
    }

    async fn _unwrap_pricing_results_a(
        &self,
        result_price: reqwest::Response,
        metric: PricingMetric,
    ) -> PricingResults {

        match metric {
            PricingMetric::PV => {
                let results_conv = result_price.json::<HashMap<String, f64>>().await;

                if results_conv.is_err() {
                    return PricingResults::PV(PortfolioType::new())
                }

                PricingResults::PV(PortfolioType(results_conv.unwrap()))
            },
            PricingMetric::PV01 => {
                let results_conv = result_price.json::<HashMap<String, HashMap<String, f64>>>().await;

                if results_conv.is_err() {
                    return PricingResults::PV01(PV01Results::new());
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert((*trade_id.clone()).to_string(), PortfolioType::from(trade_result));
                }
                PricingResults::PV01(pv01)
            },
	        PricingMetric::PnL => todo!(),
        }
    }
}



impl PriceTradeAsync for AOTrade {

    async fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    async fn price(&self, pricing_options: &MarketPricingOptions) -> Option<f64> {

        let trade_id = self.id();

        let results_pricing = self._pricing_request(
            pricing_options,
            trade_id,
            PricingMetric::PV,
        ).await;

        match results_pricing {
            Ok(result_price) => self._unwrap_pricing_results(result_price, PricingMetric::PV),
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                PricingResults::PV(PortfolioType::new())
            }
        }
    }

    async fn pv01(&self, pricing_options: &MarketPricingOptions) -> PV01Results {
        let trade_id = self.id();
        let results_pricing = self._pricing_request(
            pricing_options,
            trade_id,
            PricingMetric::PV01,
        ).await;

        match results_pricing {
            Ok(result_price) => self._unwrap_pricing_results(result_price, PricingMetric::PV01),
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                PricingResults::PV01(PV01Results::new())
            }
        }
    }
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
