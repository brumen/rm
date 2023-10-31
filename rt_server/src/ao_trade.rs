use log::{warn, error, debug,};
use std::ops::{Deref, DerefMut, };
use serde::{Serialize, Deserialize,};
use kafka::consumer::Message;
use std::collections::HashMap;

use crate::market::CurrNewMarket;
use crate::portfolio::{PV01Results, PortfolioType};
use crate::ref_deref::TryFromRef;
use crate::pricer::{
    Decoder,
    PricingMetric,
    PriceTradeAsync,
    MarketPricingOptions,
};
use crate::portfolio::PricingResults;
use crate::trade::BaseTrade;
use crate::trade::{TradeDirection, TradeError,};
use crate::ref_deref_trait;


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


#[derive(Debug, Serialize, Deserialize, Clone, PartialEq )]
pub struct AOTrade {
    pub payload: Payload,
}


impl Decoder for AOTrade {}


impl PriceTradeAsync for AOTrade {

    async fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    async fn price(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> Option<f64> {

        let trade_id = self.id();

        let results_pricing = self._pricing_request(
            PricingMetric::PV,
            pricing_options,
            curr_new_mkt,
        ).await;

        match results_pricing {
            Ok(result_price) => {
                let unwrapped_price = self._unwrap_pricing_results_a(
                    result_price,
                    PricingMetric::PV
                ).await;

                debug!("_price_ao_trade: Result = {:?}", unwrapped_price);
                if let PricingResults::PV(pv_result) = unwrapped_price {
                    let result_keys : Vec<_> = pv_result.keys().into_iter().collect();
                    // TODO: THIS IS GARBARGE
                    if result_keys.len() == 0 {
                        Some(0.)  // PortfolioType::new()
                    } else {
                        pv_result.get(result_keys[0]).copied()
                    }
                } else {
                    error!("Remote pricing of {} didnt go right!", trade_id);
                    None
                }
            },
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                None
            }
        }
    }

    async fn pv01(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PV01Results {
        let trade_id = self.id();
        let results_pricing = self._pricing_request(
            PricingMetric::PV01,
            pricing_options,
            curr_new_mkt,
        ).await;

        match results_pricing {
            Ok(result_price) => {
                let unwrapped_price = self._unwrap_pricing_results_a(result_price, PricingMetric::PV01).await;
                if let PricingResults::PV01(pv01_result) = unwrapped_price {
                    pv01_result
                } else {
                    error!("pv01: Remote PV01 of {} didnt go right. Continuing w/o priced trade.", trade_id);
                    PV01Results::new()
                }
            },
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


impl TryFromRef<Message<'_>> for AOTrade
{
    type Error = TradeError;

    fn try_from_ref(value: &Message) -> Result<Self, Self::Error> {

        let msg_utf = std::str::from_utf8(value.value)?;
        debug!("try_from_ref: Message received: {}", msg_utf);

        let msg_serialized = serde_json::from_str::<AOTrade>(msg_utf)?;
        debug!("try_from_ref: Message serialized {:?}", msg_serialized);

        Ok(msg_serialized)
    }
}

pub type AOTradeRepInner = String;

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct AOTradeRep (pub AOTradeRepInner);


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


impl PriceTradeAsync for AOTradeRep {

    async fn initial_pv(&self) -> Option<f64> {
        Some(0.)
    }

    async fn price(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> Option<f64> {

        let trade_id = self.id();

        let results_pricing = self._pricing_request(
            PricingMetric::PV,
            pricing_options,
            curr_new_mkt,
        ).await;

        match results_pricing {
            Ok(result_price) => {
                let unwrapped_price = self._unwrap_pricing_results_a(
                    result_price,
                    PricingMetric::PV
                ).await;

                debug!("_price_ao_trade: Result = {:?}", unwrapped_price);
                if let PricingResults::PV(pv_result) = unwrapped_price {
                    let result_keys : Vec<_> = pv_result.keys().into_iter().collect();
                    // TODO: THIS IS GARBARGE
                    if result_keys.len() == 0 {
                        Some(0.)  // PortfolioType::new()
                    } else {
                        pv_result.get(result_keys[0]).copied()
                    }
                } else {
                    error!("Remote pricing of {} didnt go right!", trade_id);
                    None
                }
            },
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                None
            }
        }
    }

    async fn pv01(
        &self,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PV01Results {
        let trade_id = self.id();
        let results_pricing = self._pricing_request(
            PricingMetric::PV01,
            pricing_options,
            curr_new_mkt,
        ).await;

        match results_pricing {
            Ok(result_price) => {
                let unwrapped_price = self._unwrap_pricing_results_a(result_price, PricingMetric::PV01).await;
                if let PricingResults::PV01(pv01_result) = unwrapped_price {
                    pv01_result
                } else {
                    error!("pv01: Remote PV01 of {} didnt go right. Continuing w/o priced trade.", trade_id);
                    PV01Results::new()
                }
            },
            Err(e) => {
                warn!("Trade {:?} could not price correctly: {}", trade_id, e);
                PV01Results::new()
            }
        }
    }
}
