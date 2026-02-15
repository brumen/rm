use log::warn;
use serde::Serialize;
use std::cmp::PartialOrd;
use std::default::Default;
use std::fmt::Debug;
use std::ops::{Add, AddAssign, Deref, DerefMut, Mul, MulAssign, Neg};
use std::{collections::HashMap, ops::SubAssign};

use crate::pricer::PricingMetric;
use crate::ref_deref_trait;
use crate::trade::{BaseTrade, TradeDirection};

pub type PortfolioInner = HashMap<String, f64>;

/// PortfolioType is of form (trade_id, trade_pv)
#[derive(Debug, PartialEq, Serialize, Clone, Default)]
pub struct PortfolioType(pub PortfolioInner);

ref_deref_trait!(PortfolioType, PortfolioInner);

impl PortfolioType {
    fn len(&self) -> usize {
        self.keys().count()
    }
}

// compares the two portfolios of PortfolioType
impl PartialOrd for PortfolioType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let self_less_other = self.keys().all(|key| other.contains_key(key));
        let other_less_self = other.keys().all(|key| self.contains_key(key));

        if self_less_other && other_less_self {
            return Some(std::cmp::Ordering::Equal);
        }

        if self_less_other {
            return Some(std::cmp::Ordering::Less);
        }

        if other_less_self {
            return Some(std::cmp::Ordering::Greater);
        }

        None
    }
}

impl Add for PortfolioType {
    type Output = PortfolioType;

    fn add(self, other_portfolio: PortfolioType) -> Self::Output {
        let mut new_portfolio = PortfolioInner::new();
        new_portfolio.extend((*self).clone()); // TODO: Can this be done w/o copying.

        for (trade_id, trade_value) in other_portfolio.iter() {
            if let Some(self_value) = new_portfolio.get_mut(trade_id) {
                *self_value += *trade_value;
            } else {
                // not found in
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
            } else {
                // None
                self.insert((*trade_id).clone(), -*trade_value);
            }
        }
    }
}

impl AddAssign<PortfolioType> for PortfolioType {
    fn add_assign(&mut self, other: Self) {
        for (trade_id, trade_value) in other.iter() {
            if let Some(self_value) = self.get_mut(trade_id) {
                *self_value += *trade_value;
            } else {
                // None
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
            } else {
                // None
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
            } else {
                // None
                self.insert((*trade_id).clone(), -*trade_value);
            }
        }
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
            } else {
                // not found in
                new_portf.insert((*trade_id).clone(), *trade_value);
            }
        }

        Self(new_portf)
    }
}

// AggregatedTrades
pub type AggregatedInner = HashMap<String, f64>;

#[derive(Debug, PartialEq)]
pub struct AggregatedTrades(pub AggregatedInner);

ref_deref_trait!(AggregatedTrades, AggregatedInner);

impl Mul<f64> for PortfolioType {
    type Output = PortfolioType;

    fn mul(self, rhs: f64) -> Self {
        let mut new_agg_trades = Self::default();
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

/// PV01Results is of form (trade_id, (exposure_to, exposure_amt))
pub type PV01Inner = HashMap<String, PortfolioType>;
#[derive(Clone, Debug, PartialEq)]
pub struct PV01Results(pub PV01Inner);

ref_deref_trait!(PV01Results, PV01Inner);

impl AddAssign<&PV01Results> for PV01Results {
    fn add_assign(&mut self, rhs: &PV01Results) {
        for (trade_id, trade_val) in self.iter_mut() {
            if let Some(trade_mult) = rhs.get(trade_id) {
                *trade_val += trade_mult.clone();
            } else {
                warn!(
                    "mul_assign: Could not find the multiplying factor for {}",
                    trade_id
                );
            }
        }
    }
}

impl MulAssign<&AggregatedTrades> for PV01Results {
    fn mul_assign(&mut self, rhs: &AggregatedTrades) {
        for (trade_id, trade_val) in self.iter_mut() {
            if let Some(trade_mult) = rhs.get(trade_id) {
                *trade_val *= *trade_mult;
            } else {
                warn!(
                    "mul_assign: Could not find the multiplying factor for {}",
                    trade_id
                );
            }
        }
    }
}

impl Mul<f64> for PV01Results {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self {
        let mut new_pv01 = PV01Results::new();
        for (trade_id, portfolio) in self.iter() {
            let mut inner_portf = PortfolioType::default();
            for (trade_id_inner, value) in portfolio.iter() {
                inner_portf.insert(trade_id_inner.clone(), value * rhs);
            }
            new_pv01.insert(trade_id.clone(), inner_portf);
        }

        new_pv01
    }
}

impl Neg for PV01Results {
    type Output = Self;

    fn neg(self) -> Self::Output {
        todo!()
    }
}

impl PV01Results {
    pub fn new() -> Self {
        Self(PV01Inner::new())
    }

    // aggregates the PV01 results into Portfoliotype, irrespective of trades.
    pub fn aggregate(self) -> PortfolioType {
        let mut pv01_aggs = PortfolioType::default();
        for (_, trade_pv01) in self.iter() {
            pv01_aggs += trade_pv01;
        }
        pv01_aggs
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PricingResults {
    PV(PortfolioType),
    PV01(PV01Results),
    PnL(PortfolioType), // same as PV type
}

impl PricingResults {
    #[allow(dead_code)]
    pub fn new(metric: PricingMetric) -> Self {
        match metric {
            PricingMetric::PV => Self::PV(PortfolioType::default()),
            PricingMetric::PV01 => Self::PV01(PV01Results::new()),
            PricingMetric::PnL => Self::PnL(PortfolioType::default()),
        }
    }

    // TODO: CHECK IF WE CAN DO THIS WITHOUT CLONING!!!
    #[allow(dead_code)]
    pub fn aggregate(&self) -> PortfolioType {
        match self {
            PricingResults::PV(pv) => (*pv).clone(),
            PricingResults::PV01(pv01) => (*pv01).clone().aggregate(),
            PricingResults::PnL(pnl) => (*pnl).clone(),
        }
    }
}

impl Neg for PricingResults {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            Self::PV(portfolio) => Self::PV(-portfolio),
            Self::PV01(pv01_results) => Self::PV01(-pv01_results),
            Self::PnL(pnl_results) => Self::PnL(-pnl_results),
        }
    }
}

impl Mul<f64> for PricingResults {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self {
        match self {
            Self::PV(pv_result) => Self::PV(pv_result * rhs),
            Self::PV01(pv01_result) => Self::PV01(pv01_result * rhs),
            Self::PnL(pnl_results) => Self::PnL(pnl_results * rhs),
        }
    }
}

impl Neg for PortfolioType {
    type Output = Self;

    fn neg(self) -> Self::Output {
        let mut res = Self::default();
        for (trade_id, trade_val) in self.iter() {
            res.insert(trade_id.clone(), -*trade_val); // TODO: IMPROVE HERE!!!
        }

        res
    }
}

impl AddAssign<PricingResults> for PricingResults {
    fn add_assign(&mut self, rhs: PricingResults) {
        match rhs {
            PricingResults::PV(pv_results) => {
                *self += PricingResults::PV(pv_results);
            }
            PricingResults::PV01(pv01_results) => *self += PricingResults::PV01(pv01_results),
            PricingResults::PnL(_pnl_results) => {
                todo!()
            }
        }
    }
}

impl SubAssign<PricingResults> for PricingResults {
    fn sub_assign(&mut self, rhs: PricingResults) {
        match rhs {
            PricingResults::PV(pv_results) => {
                *self -= PricingResults::PV(pv_results);
            }
            PricingResults::PV01(pv01_results) => *self -= PricingResults::PV01(pv01_results),
            PricingResults::PnL(_pnl_results) => {
                todo!()
            }
        }
    }
}

impl AddAssign<PricingResults> for PortfolioType {
    fn add_assign(&mut self, rhs: PricingResults) {
        match rhs {
            PricingResults::PV(pv_results) => {
                *self += pv_results;
            }
            PricingResults::PV01(pv01_results) => {
                for (_, trade_portf) in pv01_results.iter() {
                    *self += trade_portf;
                }
            }
            PricingResults::PnL(_pnl_results) => {
                todo!()
            }
        }
    }
}

impl SubAssign<PricingResults> for PortfolioType {
    fn sub_assign(&mut self, rhs: PricingResults) {
        match rhs {
            PricingResults::PV(pv_results) => {
                *self -= pv_results;
            }
            PricingResults::PV01(pv01_results) => {
                for (_, trade_portf) in pv01_results.iter() {
                    *self -= trade_portf;
                }
            }
            PricingResults::PnL(pnl_results) => {
                *self -= pnl_results;
            }
        }
    }
}

impl MulAssign<&AggregatedTrades> for PricingResults {
    fn mul_assign(&mut self, rhs: &AggregatedTrades) {
        match self {
            PricingResults::PV(ref mut pv_portfolio) => *pv_portfolio *= rhs,
            PricingResults::PV01(pv01_results) => {
                // go over trades and multiply each one by a factor.
                for (trade_id, trade_val) in pv01_results.iter_mut() {
                    if let Some(trade_mult) = rhs.get(trade_id) {
                        *trade_val *= *trade_mult;
                    } else {
                        warn!("Could not find the multiplying factor for {}", trade_id);
                    }
                }
            }
            PricingResults::PnL(ref mut pnl_portfolio) => *pnl_portfolio *= rhs,
        }
    }
}

/// portfolio of pricing metrics.
/// PmPortfolio - mnemonic for PricingMetric Portfolio
type PmPortfolioInner = HashMap<PricingMetric, PortfolioType>;
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PmPortfolio(PmPortfolioInner);
ref_deref_trait!(PmPortfolio, PmPortfolioInner);

impl PmPortfolio {
    pub(crate) fn new() -> Self {
        let inner_portfolio = PmPortfolioInner::new();
        Self(inner_portfolio)
    }

    pub(crate) fn count(&self) -> HashMap<PricingMetric, usize> {
        let mut pm_displ = HashMap::new();
        for (pm, pi) in self.iter() {
            pm_displ.insert(*pm, pi.len());
        }

        pm_displ
    }

    // simple display of pm.
    // for each PV/PV01
    pub(crate) fn simple(&self) -> String {
        let mut pm_displ = String::new();
        for (pm, pi) in self.iter() {
            let pm_indiv = format!("{}: {:?} ", pm, pi.len());
            pm_displ += &pm_indiv;
        }

        // if empty we should display empty dict
        if pm_displ.is_empty() {
            pm_displ = String::from("{}");
        }
        pm_displ
    }

    pub(crate) fn assign(&mut self, other: PmPortfolio) {
        for (pm, comp_portf_pm) in other.iter() {
            match self.get_mut(pm) {
                Some(portf_pm) => {
                    *portf_pm = comp_portf_pm.clone();
                }
                None => {
                    // TODO: CHECK HERE - PROBABLY COULD BE REMOVED default()
                    let mut portfolio_pm = PortfolioType::default();
                    portfolio_pm += comp_portf_pm;
                    self.insert(*pm, portfolio_pm);
                }
            }
        }
    }
}

// TODO:
//   this determines when a PmPortfolio is accepted.
impl PartialOrd for PmPortfolio {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let self_less_other = self.keys().all(|key| other.contains_key(key));
        let other_less_self = other.keys().all(|key| self.contains_key(key));

        // TODO: THIS IS REALLY ASSOCIATED w/ EQUAL
        // if self_less_other && other_less_self {
        //     let mut pm_orders = Vec::<bool>::new();

        //     for pm in self.keys() {
        //         let portf_comp = self.get(pm) <= other.get(pm);
        //         pm_orders.push(portf_comp);
        //     }

        //     if pm_orders.iter().all(|pm_ord| *pm_ord) {
        //         return Some(std::cmp::Ordering::Less); // TODO: could also be ==
        //     }
        // }

        if self_less_other {
            let mut pm_orders = Vec::<bool>::new();

            for pm in self.keys() {
                let portf_comp = self.get(pm) <= other.get(pm);
                pm_orders.push(portf_comp);
            }

            if pm_orders.iter().all(|pm_ord| *pm_ord) {
                return Some(std::cmp::Ordering::Less); // TODO: could also be ==
            }
        }

        if other_less_self {
            let mut pm_orders = Vec::<bool>::new();

            for pm in other.keys() {
                let portf_comp = other.get(pm) <= self.get(pm);
                pm_orders.push(portf_comp);
            }

            if pm_orders.iter().all(|pm_ord| *pm_ord) {
                return Some(std::cmp::Ordering::Greater); // TODO: could also be ==
            }
        }

        None
    }
}

// impl AddAssign<PmPortfolio> for PmPortfolio {
//     fn add_assign(&mut self, other: PmPortfolio) {
//         for (pricing_metric, pm_portfolio) in other.iter() {
//             if self.contains_key(pricing_metric) {
//                 // TODO: THESE CLONES ARE SUPER POOR!!!!
//                 let curr_pm = self.get(pricing_metric).unwrap().clone();
//                 self.insert(*pricing_metric, curr_pm.clone() + pm_portfolio.clone());
//             } else {
//                 self.insert(*pricing_metric, pm_portfolio.clone());
//             }
//         }
//     }
// }

// TODO: CHECK THIS EFFICIENCY
impl AddAssign<PmPortfolio> for PmPortfolio {
    fn add_assign(&mut self, other: PmPortfolio) {
        for (pm, comp_portf_pm) in other.iter() {
            match self.get_mut(pm) {
                Some(portf_pm) => {
                    *portf_pm += comp_portf_pm;
                }
                None => {
                    let mut portfolio_pm = PortfolioType::default();
                    portfolio_pm += comp_portf_pm;
                    self.insert(*pm, portfolio_pm);
                }
            }
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

        let mut portfolio_1 = PortfolioType::from([('1'.to_string(), 10.), ('2'.to_string(), 20.)]);
        let portfolio_2 = PortfolioType::from([('1'.to_string(), 20.)]);
        portfolio_1 += portfolio_2;
        let portfolio_res = PortfolioType::from([('1'.to_string(), 30.), ('2'.to_string(), 20.)]);

        assert_eq!(portfolio_1, portfolio_res);
    }
}
