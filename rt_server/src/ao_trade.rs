use tracing::{info, debug, error, warn};
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use std::marker::Sync;
use std::ops::{Deref, DerefMut};
use ractor::async_trait;
use std::fmt;

use crate::portfolio::PV01Results;
use crate::portfolio::PricingResults;
use crate::pricer::{Decoder, MarketPricingOptions, PriceTradeAsync, PricingMetric, PriceTrade};
//use crate::process_trade::ProcessTradeValue;
use crate::ref_deref::{TryFromRef, TryFromRef2};
use crate::ref_deref_trait;
use crate::trade::BaseTrade;
use crate::trade::TradeDirection;
use crate::ao_market::{AOMarketType, AOMarketParams};

// structure of the AOTrade payload, possibly can be simplified.
//

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Payload {
    pub op: String,
    pub after: AfterPosition,
    pub before: Option<BeforePosition>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct AfterPosition {
    pub position_id: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct BeforePosition {
    pub position_id: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct AOTrade {
    pub payload: Payload,
}

impl Decoder for AOTrade {}
impl TryFromRef2 for AOTrade {}

impl fmt::Display for AOTrade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let pos_id = self.payload.after.position_id;
        write!(f, "{}", pos_id)
    }
}


impl PriceTrade<AOMarketParams> for AOTrade {
    async fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    async fn price(
        &self,
        marlet: &AOMarketType,
    ) -> Option<f64> {
        let trade_id = self.id();
        let results_pricing = self
            ._pricing_request(PricingMetric::PV, pricing_options, curr_new_mkt)
            .await;

        match results_pricing {
            Ok(result_price) => {
                let unwrapped_price = self
                    ._unwrap_pricing_results_a(result_price, PricingMetric::PV)
                    .await;

                debug!("_price_ao_trade: Result = {:?}", unwrapped_price);
                if let PricingResults::PV(pv_result) = unwrapped_price {
                    let result_keys: Vec<_> = pv_result.keys().collect();
                    // TODO: THIS IS GARBARGE
                    if result_keys.is_empty() {
                        Some(0.) // PortfolioType::new()
                    } else {
                        pv_result.get(result_keys[0]).copied()
                    }
                } else {
                    error!("Remote pricing of {} didnt go right!", trade_id);
                    None
                }
            }
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                None
            }
        }
    }



    async fn pv01(&self, market: &AOMarketType) -> PV01Results {
        todo!()
    }
}



// #[async_trait]
// impl ProcessTradeValue for AOTrade {
//     async fn value_by_metric2(
//         &self,
//         metric: PricingMetric,
//         pricing_options: &MarketPricingOptions,
//         curr_new_mkt: &MarketType,
//     ) -> PricingResults {

//         self.value_by_metric(metric, pricing_options, curr_new_mkt)
//             .await
//     }
// }



impl<T: BaseTrade + Decoder + Sync> PriceTradeAsync for T {
    async fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    async fn price(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> Option<f64> {
        let trade_id = self.id();
        let results_pricing = self
            ._pricing_request(PricingMetric::PV, pricing_options, curr_new_mkt)
            .await;

        match results_pricing {
            Ok(result_price) => {
                let unwrapped_price = self
                    ._unwrap_pricing_results_a(result_price, PricingMetric::PV)
                    .await;

                debug!("_price_ao_trade: Result = {:?}", unwrapped_price);
                if let PricingResults::PV(pv_result) = unwrapped_price {
                    let result_keys: Vec<_> = pv_result.keys().collect();
                    // TODO: THIS IS GARBARGE
                    if result_keys.is_empty() {
                        Some(0.) // PortfolioType::new()
                    } else {
                        pv_result.get(result_keys[0]).copied()
                    }
                } else {
                    error!("Remote pricing of {} didnt go right!", trade_id);
                    None
                }
            }
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                None
            }
        }
    }

    async fn pv01(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: &MarketType,
    ) -> PV01Results {
        let trade_id = self.id();
        let results_pricing = self
            ._pricing_request(PricingMetric::PV01, pricing_options, curr_new_mkt)
            .await;

        match results_pricing {
            Ok(result_price) => {
                let unwrapped_price = self
                    ._unwrap_pricing_results_a(result_price, PricingMetric::PV01)
                    .await;
                if let PricingResults::PV01(pv01_result) = unwrapped_price {
                    pv01_result
                } else {
                    error!(
                        "pv01: Remote PV01 of {} didnt go right. Continuing w/o priced trade.",
                        trade_id
                    );
                    PV01Results::new()
                }
            }
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                PV01Results::new()
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

pub type AOTradeRepInner = String;

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct AOTradeRep(pub AOTradeRepInner);

ref_deref_trait!(AOTradeRep, AOTradeRepInner);

impl Decoder for AOTradeRep {}

impl BaseTrade for AOTradeRep {
    fn id(&self) -> String {
        self.to_string()
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}
