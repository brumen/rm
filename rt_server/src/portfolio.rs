use log::warn;
use serde::Serialize;
use std::cmp::PartialOrd;
use std::default::Default;
use std::fmt::Debug;
use std::ops::{Add, AddAssign, Deref, DerefMut, Mul, MulAssign, Neg};
use std::{
    collections::{HashMap, HashSet},
    ops::SubAssign,
};

use crate::pricer::PricingMetric;
use crate::ref_deref_trait;
use crate::trade::{BaseTrade, TradeDirection};

pub type PortfolioInner = HashMap<String, f64>;
type TradesLocal = HashSet<String>;
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

    pub(crate) fn assign_metric(&mut self, metric: &PricingMetric, portfolio: PortfolioType) {
        match self.get_mut(metric) {
            Some(portfolio_pm) => {
                *portfolio_pm += portfolio;
            }
            None => {
                self.insert(*metric, portfolio);
            }
        }
    }

    // returns the list of trades from one of the PM - they should
    //    all be the same.
    pub(crate) fn get_trades(&self) -> TradesLocal {
        if let Some((_, pricing_results)) = self.iter().next() {
            pricing_results.keys().cloned().collect::<TradesLocal>()
        } else {
            TradesLocal::new()
        }
    }
}

//  Used for determining when there is an ordering between two portfolios.
//     and when one portfolio is accepted.
impl PartialOrd for PmPortfolio {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let self_less_other = self.keys().all(|key| other.contains_key(key));
        let other_less_self = other.keys().all(|key| self.contains_key(key));

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
    use crate::portfolio::{PmPortfolio, PortfolioType};
    use crate::pricer::PricingMetric;
    use std::cmp::Ordering;

    #[test]
    fn portfolio_works_1() {
        // tests whether += works for 2 portfolios.

        let mut portfolio_1 = PortfolioType::from([("1".to_string(), 10.), ("2".to_string(), 20.)]);
        let portfolio_2 = PortfolioType::from([("1".to_string(), 20.)]);
        portfolio_1 += portfolio_2;
        let portfolio_res = PortfolioType::from([("1".to_string(), 30.), ("2".to_string(), 20.)]);

        assert_eq!(portfolio_1, portfolio_res);
    }

    #[test]
    fn partial_cmp_equal_when_same_keys() {
        // values do not matter; ordering is based on key containment.
        let p1 = PortfolioType::from([("1".to_string(), 10.), ("2".to_string(), 20.)]);
        let p2 = PortfolioType::from([("1".to_string(), -999.), ("2".to_string(), 0.)]);

        assert_eq!(p1.partial_cmp(&p2), Some(Ordering::Equal));
        assert_eq!(p2.partial_cmp(&p1), Some(Ordering::Equal));
    }

    #[test]
    fn partial_cmp_less_when_subset_of_keys() {
        let smaller = PortfolioType::from([("1".to_string(), 10.)]);
        let bigger = PortfolioType::from([("1".to_string(), 10.), ("2".to_string(), 20.)]);

        assert_eq!(smaller.partial_cmp(&bigger), Some(Ordering::Less));
        assert_eq!(bigger.partial_cmp(&smaller), Some(Ordering::Greater));
    }

    #[test]
    fn partial_cmp_none_when_incomparable_key_sets() {
        let p1 = PortfolioType::from([("1".to_string(), 10.)]);
        let p2 = PortfolioType::from([("2".to_string(), 20.)]);

        assert_eq!(p1.partial_cmp(&p2), None);
        assert_eq!(p2.partial_cmp(&p1), None);
    }

    #[test]
    fn partial_cmp_less_for_empty_vs_nonempty() {
        let empty = PortfolioType::default();
        let nonempty = PortfolioType::from([("1".to_string(), 10.)]);

        assert_eq!(empty.partial_cmp(&nonempty), Some(Ordering::Less));
        assert_eq!(nonempty.partial_cmp(&empty), Some(Ordering::Greater));
    }

    #[test]
    fn partial_cmp_equal_for_both_empty() {
        let p1 = PortfolioType::default();
        let p2 = PortfolioType::default();

        assert_eq!(p1.partial_cmp(&p2), Some(Ordering::Equal));
        assert_eq!(p2.partial_cmp(&p1), Some(Ordering::Equal));
    }

    #[test]
    fn pmportfolio_assign_metric_inserts_when_missing() {
        let mut pmp = PmPortfolio::new();
        let pv = PortfolioType::from([("t1".to_string(), 1.0)]);

        pmp.assign_metric(&PricingMetric::PV, pv.clone());

        assert_eq!(pmp.get(&PricingMetric::PV), Some(&pv));
    }

    #[test]
    fn pmportfolio_assign_metric_adds_when_existing() {
        let mut pmp = PmPortfolio::new();
        pmp.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 1.0)]),
        );

        pmp.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 2.0), ("t2".to_string(), 10.0)]),
        );

        let expected = PortfolioType::from([("t1".to_string(), 3.0), ("t2".to_string(), 10.0)]);
        assert_eq!(pmp.get(&PricingMetric::PV), Some(&expected));
    }

    #[test]
    fn pmportfolio_add_assign_merges_metrics_and_adds_values() {
        let mut a = PmPortfolio::new();
        a.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 1.0)]),
        );

        let mut b = PmPortfolio::new();
        b.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 2.0), ("t2".to_string(), 10.0)]),
        );
        b.assign_metric(
            &PricingMetric::PnL,
            PortfolioType::from([("t1".to_string(), -1.0)]),
        );

        a += b;

        let expected_pv = PortfolioType::from([("t1".to_string(), 3.0), ("t2".to_string(), 10.0)]);
        let expected_pnl = PortfolioType::from([("t1".to_string(), -1.0)]);
        assert_eq!(a.get(&PricingMetric::PV), Some(&expected_pv));
        assert_eq!(a.get(&PricingMetric::PnL), Some(&expected_pnl));
    }

    #[test]
    fn pmportfolio_partial_cmp_less_when_metric_keys_subset_and_each_portfolio_ordered() {
        // a has subset of metrics vs b; and within common metrics, a's portfolios are subsets of b's.
        let mut a = PmPortfolio::new();
        a.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 1.0)]),
        );

        let mut b = PmPortfolio::new();
        b.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 999.0), ("t2".to_string(), 2.0)]),
        );
        b.assign_metric(
            &PricingMetric::PnL,
            PortfolioType::from([("t1".to_string(), 0.0)]),
        );

        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
        assert_eq!(b.partial_cmp(&a), Some(Ordering::Greater));
    }

    #[test]
    fn pmportfolio_partial_cmp_none_when_common_metric_portfolios_incomparable() {
        // both share PV, but their PV portfolios are incomparable (different keys).
        let mut a = PmPortfolio::new();
        a.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 1.0)]),
        );

        let mut b = PmPortfolio::new();
        b.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t2".to_string(), 2.0)]),
        );

        assert_eq!(a.partial_cmp(&b), None);
        assert_eq!(b.partial_cmp(&a), None);
    }

    #[test]
    fn pmportfolio_count_reports_trade_counts_per_metric() {
        let mut pmp = PmPortfolio::new();
        pmp.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 1.0), ("t2".to_string(), 2.0)]),
        );
        pmp.assign_metric(
            &PricingMetric::PnL,
            PortfolioType::from([("t1".to_string(), -1.0)]),
        );

        let counts = pmp.count();
        assert_eq!(counts.get(&PricingMetric::PV), Some(&2usize));
        assert_eq!(counts.get(&PricingMetric::PnL), Some(&1usize));
    }

    #[test]
    fn pmportfolio_simple_empty_is_braces() {
        let pmp = PmPortfolio::new();
        assert_eq!(pmp.simple(), "{}".to_string());
    }

    #[test]
    fn pmportfolio_simple_includes_metric_names() {
        let mut pmp = PmPortfolio::new();
        pmp.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 1.0)]),
        );

        let s = pmp.simple();
        // Formatting uses Display for PricingMetric; just ensure it includes the metric label and count.
        assert!(s.contains("PV"));
        assert!(s.contains("1"));
    }

    #[test]
    fn pmportfolio_assign_overwrites_existing_metric_portfolio() {
        let mut a = PmPortfolio::new();
        a.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t1".to_string(), 1.0)]),
        );

        let mut b = PmPortfolio::new();
        b.assign_metric(
            &PricingMetric::PV,
            PortfolioType::from([("t2".to_string(), 2.0)]),
        );

        a.assign(b);

        // assign() replaces existing PV with b's PV.
        let expected = PortfolioType::from([("t2".to_string(), 2.0)]);
        assert_eq!(a.get(&PricingMetric::PV), Some(&expected));
    }
}
