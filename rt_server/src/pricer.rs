use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use tracing::{debug, warn};
use ractor::async_trait;
use std::sync::Arc;

use crate::market::{MarketTypeT};
use crate::portfolio::{PV01Results, PortfolioType, PricingResults};
use crate::trade::{BaseTrade, TradeRep, TradeDirection};

// which metric to compute
#[derive(Debug, Clone, Copy)]
pub enum PricingMetric {
    PV,
    PV01,
    PnL,
}

impl fmt::Display for PricingMetric {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PricingMetric::PV => write!(f, "PV"),
            PricingMetric::PV01 => write!(f, "PV01"),
            PricingMetric::PnL => write!(f, "PnL"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PricingStruct {
    pub nb_sim: i32,
    pub default_price: f64,
}


// Decoder decodes the pricing results from a trade and
#[async_trait]
pub trait Decoder {
    /// unwraps the pricing results
    fn _unwrap_pricing_results(
        &self,
        result_price: reqwest::blocking::Response,
        metric: PricingMetric,
    ) -> PricingResults {
        match metric {
            PricingMetric::PV => {
                let results_conv = result_price.json::<HashMap<String, f64>>();

                if results_conv.is_err() {
                    return PricingResults::PV(PortfolioType::default());
                }

                PricingResults::PV(PortfolioType(results_conv.unwrap()))
            }

            PricingMetric::PV01 => {
                let results_conv = result_price.json::<HashMap<String, HashMap<String, f64>>>();

                if results_conv.is_err() {
                    return PricingResults::PV01(PV01Results::new());
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert(
                        (*trade_id.clone()).to_string(),
                        PortfolioType::from(trade_result),
                    );
                }
                PricingResults::PV01(pv01)
            }

            PricingMetric::PnL => todo!(),
        }
    }

    async fn _unwrap_pricing_results_a(
        &self,
        result_price: reqwest::Response,
        metric: PricingMetric,
    ) -> PricingResults  {
        match metric {
            PricingMetric::PV => {
                let results_conv = result_price.json::<HashMap<String, f64>>().await;

                if results_conv.is_err() {
                    return PricingResults::PV(PortfolioType::default());
                }

                PricingResults::PV(PortfolioType(results_conv.unwrap()))
            }

            PricingMetric::PV01 => {
                let results_conv = result_price
                    .json::<HashMap<String, HashMap<String, f64>>>()
                    .await;

                if results_conv.is_err() {
                    return PricingResults::PV01(PV01Results::new());
                }

                let mut pv01 = PV01Results::new();
                for (trade_id, trade_result) in results_conv.unwrap().iter() {
                    let _ = pv01.insert(
                        (*trade_id.clone()).to_string(),
                        PortfolioType::from(trade_result),
                    );
                }
                PricingResults::PV01(pv01)
            }

            PricingMetric::PnL => todo!(),
        }
    }
}


// market type T send and sync version,
pub(crate) type MarketTypeTSend<MP> = dyn MarketTypeT<MP=MP> + Send + Sync;

// MP are market parameters, () if none.
// MT is market type, depending on the market parameters.
#[async_trait]
pub trait PriceTrade<MP>: BaseTrade + Send + Sync
where
    dyn MarketTypeT<MP=MP>: Send + Sync,
    // for <'a> &'a (dyn MarketTypeT<MP=MP> + Send + Sync): MarketTypeT,
    MP: 'static + Send,
{

    async fn initial_pv(&self) -> Option<f64> where Self: Send;
    async fn price(&self, market: Arc<&MarketTypeTSend<MP>> ) -> Option<f64>;
    async fn pv01(&self, market: Arc<&MarketTypeTSend<MP>> ) -> PV01Results;
    async fn pnl(&self, market: Arc<&MarketTypeTSend<MP>>) -> Option<f64> {
        let initial_pv_val = self.initial_pv().await?;

        self.price(market)
            .await
            .map(|curr_price| curr_price - initial_pv_val)
    }

    /// values the trade for a specific metric.
    #[allow(dead_code)]
    async fn value_by_metric(
        &self,
        metric: PricingMetric,
        market: Arc<&MarketTypeTSend<MP>>,
    ) -> PricingResults {

        let trade_name = self.id();

        match metric {
            PricingMetric::PV => {
                let priced_trade = self.price(market).await;
                debug!("_value_trade: PV of {:?} = {:?}", trade_name, priced_trade);
                if let Some(price_trade) = priced_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, price_trade)]))
                } else {
                    PricingResults::PV(PortfolioType::default())
                }
            }

            PricingMetric::PV01 => {
                let trade_pv01 = self.pv01(market).await;
                debug!("_value_trade: PV01 of {:?} = {:?}", trade_name, trade_pv01);
                PricingResults::PV01(trade_pv01)
            }

            PricingMetric::PnL => {
                let pnl_trade = self.pnl(market).await;
                debug!("_value_trade: PnL of {:?} = {:?}", trade_name, pnl_trade);
                if let Some(pnl_trade_real) = pnl_trade {
                    PricingResults::PV(PortfolioType::from([(trade_name, pnl_trade_real)]))
                } else {
                    PricingResults::PV(PortfolioType::default())
                }
            }
        }
    }
}

// This is not important, but was implemented to implement PriceTrade
impl<TR> BaseTrade for TradeRep<TR> {
    fn id(&self) -> String {
        "TradeRep".to_string()  // PERHAPS FIX THIS.
    }

    fn direction(&self) -> TradeDirection {
        TradeDirection::Create
    }
}


// if we have PriceTrade implementation for TR
//   then we have the PriceTrade implementation for TradeReduce
// MT: MarketTypeT<MP>
// TR: trade representation.
#[async_trait]
impl<MP, TR> PriceTrade<MP> for TradeRep<TR>
where
    TR: PriceTrade<MP> + std::fmt::Debug + Send + Sync,
    MP: 'static + Send + Sync,
    dyn MarketTypeT<MP=MP>: Send + Sync,
{

    async fn initial_pv(&self) -> Option<f64> {
        let mut portf_val = 0.;
        for indiv_trade in self.iter()  {
            let (_trade_name, trade_v) = indiv_trade.pair();

            let tv = trade_v.initial_pv().await; // tv = trade value
            match tv {
                None => {
                    warn!("Could not initial_pv of {:?}", trade_v);
                },
                Some(tv_real) => {
                    portf_val += tv_real;
                },
            }
        }
        Some(portf_val)
    }

    async fn price(&self, market: Arc<&MarketTypeTSend<MP>>) -> Option<f64> {
        let mut portf_val = 0.;
        for indiv_trade in self.iter()  {
            let (_trade_name, trade_v) = indiv_trade.pair();

            let tv = trade_v.price(market.clone()).await; // only clonging the Arc
            match tv {
                None => {
                    warn!("Could not price of {:?}", trade_v);
                },
                Some(tv_real) => {
                    portf_val += tv_real;
                },
            }
        }
        Some(portf_val)
    }

    async fn pv01(&self, market: Arc<&MarketTypeTSend<MP>>) -> PV01Results {
        let mut portf_val = PV01Results::new();
        for indiv_trade in self.iter()  {
            let (_trade_name, trade_v) = indiv_trade.pair();

            let tv = trade_v.pv01(market.clone()).await;
            portf_val += &tv;
        }
        portf_val
    }
}
