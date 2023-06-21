use std::fmt::Debug;
use log::{warn, debug,};
use std::{collections::HashMap, ops::SubAssign};
use std::ops::{Add, AddAssign, Mul, MulAssign, };
use serde::Serialize;
use std::sync::mpsc::Sender;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, };


use std::ops::{Deref, DerefMut,};
use crate::ref_deref_trait;
use crate::ref_deref::TryFromRef;
use crate::trade::{TradeDirection, BaseTrade, TradeAggregation, };
use crate::streaming::Streaming;


pub type PortfolioInner = HashMap<String, f64>;

#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct PortfolioType ( pub PortfolioInner );


ref_deref_trait!(PortfolioType, PortfolioInner);


impl Add for PortfolioType {
    type Output = PortfolioType;

    fn add(self, other_portfolio : PortfolioType) -> Self::Output {

        let mut new_portfolio = PortfolioInner::new();
        new_portfolio.extend((*self).clone());  // TODO: Can this be done w/o copying.

        for (trade_id, trade_value) in other_portfolio.iter() {
            if let Some(self_value) = new_portfolio.get_mut(trade_id) {
                *self_value += *trade_value;
            } else {  // not found in
                new_portfolio.insert((*trade_id).clone(), *trade_value);
            }
        }
        PortfolioType(new_portfolio)
    }
}


impl SubAssign for PortfolioType {
    fn sub_assign(&mut self, rhs: Self) {
        // negate the values of
        for (trade_id, trade_value) in rhs.iter() {
            if let Some(self_value) = self.get_mut(trade_id) {
                *self_value -= *trade_value;
            } else {  // None
                self.insert((*trade_id).clone(), - *trade_value);
            }
        }
    }
}

impl AddAssign<PortfolioType> for PortfolioType {
    fn add_assign(&mut self, other: Self) {
        for (trade_id, trade_value) in other.iter() {
            if let Some(self_value) = self.get_mut(trade_id) {
                *self_value += *trade_value;
            } else {  // None
                self.insert((*trade_id).clone(), *trade_value);
            }
        }
    }
}

impl AddAssign<&PortfolioType> for PortfolioType {

    fn add_assign(&mut self, other: &Self) {
        for (trade_id, trade_value) in other.iter() {
            if let Some(self_value) = self.get_mut(trade_id) {
                *self_value += *trade_value;
            } else {  // None
                self.insert((*trade_id).clone(), *trade_value);
            }
        }
    }
}

impl SubAssign<&PortfolioType> for PortfolioType {

    fn sub_assign(&mut self, other: &Self) {
        for (trade_id, trade_value) in other.iter() {
            if let Some(self_value) = self.get_mut(trade_id) {
                *self_value -= *trade_value;
            } else {  // None
                self.insert((*trade_id).clone(), - *trade_value);
            }
        }
    }
}


impl PortfolioType {
    pub fn new() -> Self {
        Self(PortfolioInner::new())
    }
}

impl<const N: usize> From<[(String, f64); N]> for PortfolioType {
    fn from(arr: [(String, f64); N]) -> Self {
        Self(PortfolioInner::from(arr))
    }
}

impl From<&HashMap<String, f64>> for PortfolioType {
    fn from(other_portfolio: &HashMap<String, f64>) -> Self {
        let mut new_portf = PortfolioInner::new();
        for (trade_id, trade_value) in other_portfolio.iter() {
            if let Some(self_value) = new_portf.get_mut(trade_id) {
                *self_value += *trade_value;
            } else {  // not found in
                new_portf.insert((*trade_id).clone(), *trade_value);
            }
        }

        Self(new_portf)
    }
}


// AggregatedTrades
pub type AggregatedInner = HashMap<String, f64>;

#[derive(Debug, PartialEq)]
pub struct AggregatedTrades ( pub AggregatedInner );

ref_deref_trait!(AggregatedTrades, AggregatedInner);

impl Mul<f64> for PortfolioType {
    type Output = PortfolioType;

    fn mul(self, rhs: f64) -> Self {
        let mut new_agg_trades = Self::new();
        for (trade_id, trade_val) in self.iter() {
            new_agg_trades.insert(trade_id.clone(), *trade_val * rhs);
        }
        new_agg_trades
    }
}

impl MulAssign<&AggregatedTrades> for PortfolioType {
    fn mul_assign(&mut self, rhs: &AggregatedTrades) {
        for (trade_id, trade_val) in self.iter_mut() {

            //if let Ok(tid) = trade_id.parse::<u16>() {
            if let Some(trade_mult) = rhs.get(trade_id) {
                *trade_val *= *trade_mult;
            } else {
                warn!("Could not find the multiplying factor for {}", trade_id);
            }
            //} else {
            //    warn!("Could not convert {:?} to u16", trade_id);
            //}
        }
    }
}

impl MulAssign<f64> for PortfolioType {
    fn mul_assign(&mut self, rhs: f64) {
        for (_, trade_val) in self.iter_mut() {
            *trade_val *= rhs;
        }
    }
}

impl AggregatedTrades {
    pub fn new() -> Self {
        Self(AggregatedInner::new())
    }

    pub fn len(&self) -> usize {
        self.0.keys().len()

    }
}


impl<TT: BaseTrade> AddAssign<TT> for AggregatedTrades {
    fn add_assign(&mut self, rhs: TT) {
        let new_trade_id = rhs.id();
        let new_trade_position = match rhs.direction() {
            TradeDirection::Create => 1.,
            TradeDirection::Delete => -1.,
            _ => 0.,
        };

        if let Some(agg_pos) = self.get_mut(&new_trade_id) {
            *agg_pos += new_trade_position;
            if *agg_pos == 0. {
                let _ = self.remove(&new_trade_id);
            }
        } else {
            self.insert(new_trade_id, new_trade_position);
        }
    }
}


impl<TT: BaseTrade> Add<TT> for AggregatedTrades {
    type Output = AggregatedTrades;

    fn add(mut self, rhs: TT) -> Self::Output {
        let new_trade_id = rhs.id();
        let new_trade_position = match rhs.direction() {
            TradeDirection::Create => 1.,
            TradeDirection::Delete => -1.,
            _ => 0.,
        };

        if let Some(agg_pos) = self.get_mut(&new_trade_id) {
            *agg_pos += new_trade_position;
            if *agg_pos == 0. {
                let _ = self.remove(&new_trade_id);
            }
        } else {
            self.insert(new_trade_id, new_trade_position);
        }

        self
    }
}


// PV01Results
pub type PV01Inner = HashMap<String, PortfolioType>;
#[derive(Clone, Debug)]
pub struct PV01Results ( pub PV01Inner );

ref_deref_trait!(PV01Results, PV01Inner);

impl MulAssign<&AggregatedTrades> for PV01Results {
    fn mul_assign(&mut self, rhs: &AggregatedTrades) {
        for (trade_id, trade_val) in self.iter_mut() {

            //if let Ok(tid) = trade_id.parse::<u16>() {
            if let Some(trade_mult) = rhs.get(trade_id) {
                *trade_val *= *trade_mult;
            } else {
                warn!("Could not find the multiplying factor for {}", trade_id);
            }
            //} else {
            //    warn!("Could not convert {:?} to u16", trade_id);
            //}
        }
    }
}

impl Mul<f64> for PV01Results {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self {
        let mut new_pv01 = PV01Results::new();
        for (trade_id, portfolio) in self.iter() {
            let mut inner_portf = PortfolioType::new();
            for (trade_id_inner, value) in portfolio.iter() {
                inner_portf.insert(trade_id_inner.clone(), value * rhs);
            }
            new_pv01.insert(trade_id.clone(), inner_portf);
        }

        new_pv01
    }
}

impl PV01Results {
    pub fn new() -> Self {
        Self(PV01Inner::new())
    }

    // pub fn from_results(&mut self, results:
    // let mut pv01 = PV01Results::new();
    // for (trade_id, trade_result) in results_conv.unwrap().iter() {
    //     let _ = pv01.insert((*trade_id.clone()).to_string(), PortfolioType::from(trade_result));
    // }
    // PricingResults::PV01(pv01)


    // aggregates the PV01 results into Portfoliotype, irrespective of trades.
    pub fn aggregate(self) -> PortfolioType {
        let mut pv01_aggs = PortfolioType::new();
        for (_, trade_pv01) in self.iter() {
            pv01_aggs += trade_pv01;
        }
        pv01_aggs
    }
}


#[derive(Debug)]
pub enum PricingResults {
    PV(PortfolioType),
    PV01(PV01Results),
}

impl Mul<f64> for PricingResults {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self {

        match self {
            Self::PV(pv_result) => Self::PV(pv_result * rhs),
            Self::PV01(pv01_result) => Self::PV01(pv01_result * rhs),
        }
    }
}

impl AddAssign<PricingResults> for PortfolioType {

    fn add_assign(&mut self, rhs: PricingResults) {

        match rhs {
            PricingResults::PV(pv_results) => {
                *self += pv_results;
            },
            PricingResults::PV01(pv01_results) => {
                for (_, trade_portf) in pv01_results.iter() {
                    *self += trade_portf;
                }
            },
        }
    }
}

impl SubAssign<PricingResults> for PortfolioType {

    fn sub_assign(&mut self, rhs: PricingResults) {

        match rhs {
            PricingResults::PV(pv_results) => {
                *self -= pv_results;
            },
            PricingResults::PV01(pv01_results) => {
                for (_, trade_portf) in pv01_results.iter() {
                    *self -= trade_portf;

                }
            },
        }
    }
}


impl MulAssign<&AggregatedTrades> for PricingResults {

    fn mul_assign(&mut self, rhs: &AggregatedTrades) {

        match self {
            PricingResults::PV(ref mut portfolio) => *portfolio *= rhs,
            PricingResults::PV01(pv01_results) => {
                // go over trades and multiply each one by a factor.
                for (trade_id, trade_val) in pv01_results.iter_mut() {

                    //if let Ok(tid) = trade_id.parse::<u16>() {
                    if let Some(trade_mult) = rhs.get(trade_id) {
                        *trade_val *= *trade_mult;
                    } else {
                        warn!("Could not find the multiplying factor for {}", trade_id);
                    }
                    //} else {
                    //    warn!("Could not convert {:?} to u16", trade_id);
                    //}
                }

            }
        }

    }
}


pub trait PortfolioSender : TradeAggregation
{
    fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TT>,
        sender_curr: Sender<Self::TT>,
        pos_topic: String,
    );
}


impl<T> PortfolioSender for T
where
    T: Streaming + TradeAggregation,
    //TT: for<'a> TryFromRef<Message<'a>> + std::fmt::Debug + Send + Clone,
    //for<'a> <TT as TryFromRef<Message<'a>>>::Error: Debug,
{
    fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TT>,
        sender_curr: Sender<Self::TT>,
        pos_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());

        let mut pos_listener = Consumer::from_hosts(vec![bootstrap_servers,])
            .with_topic_partitions(pos_topic, &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        loop {
            for ms in pos_listener.poll().unwrap().iter() {
                debug!("__construct_portfolio: got some messages");
                for msg in ms.messages() {
                    debug!("__construct_portfolio: {:?}",  msg);

                    match Self::TT::try_from_ref(msg) {
                        Err(e) => {
                            warn!("__construct_portfolio: Problem w/ trade: {:?}", e);
                            continue;
                        },
                        Ok(trade) => {
                            debug!("__construct_portfolio: sending trade {:?}", trade);
                            let _ = sender_new.send(trade.clone());
                            let _ = sender_curr.send(trade);
                        },
                    }
                }
                let _ = pos_listener.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            pos_listener.commit_consumed().unwrap();
        }
    }
}



#[cfg(test)]
mod portfolio_tests {
    use time::{Date, Month};

    use crate::portfolio::PortfolioType;

    #[test]
    fn portfolio_works_1() {
        // tests whether += works for 2 portfolios.

        let date_1 = Date::from_calendar_date(2023, Month::January, 10).unwrap();
        let date_2 = Date::from_calendar_date(2023, Month::February, 20).unwrap();
        let date_3 = date_1.clone();
        let date_4 = date_1.clone();
        let date_5 = date_2.clone();
        let mut portfolio_1 = PortfolioType::from([('1'.to_string(), 10.), ('2'.to_string(), 20.),]);
        let portfolio_2 = PortfolioType::from([('1'.to_string(), 20.),]);
        portfolio_1 += portfolio_2;
        let portfolio_res = PortfolioType::from([('1'.to_string(), 30.), ('2'.to_string(), 20.),]);

        assert_eq!(portfolio_1, portfolio_res);
    }
}
