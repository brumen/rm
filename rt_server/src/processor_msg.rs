use dashmap::DashMap;
/// messages for the Multiple Actor references.
use ractor::ActorRef;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use strum::{AsRefStr, EnumDiscriminants, IntoStaticStr};

use crate::portfolio::PmPortfolio;
use crate::pricer::PricingMetric;
use crate::ref_deref_trait;

pub(crate) type TradesLocal = HashSet<String>;

/// message that the new processor receives
#[derive(EnumDiscriminants)]
#[strum_discriminants(name(ProcessorMiddleMessageStates))] // Renames the generated enum
#[strum_discriminants(derive(std::hash::Hash))] // Adds hash trait to ProcessorMiddleMessageStates
#[strum_discriminants(derive(strum::Display))] // Adds display trait to ProcessorMiddleMessageStates
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(dead_code)]
#[derive(Clone, Debug, AsRefStr)]
pub(crate) enum ProcessorMiddleMessage<MT> {
    NewTrade(String), // message from trade producer, trade id.
    NewMarket(MT),    // message from market handler, market_name
    // message from Processor_below:
    //    1st arg: market that we are evaluating
    //    2nd arg: missing trades that we still need -
    //                if Vec.is_empty() then we accepted the portfolio, otherwise market is not accepted.
    Behind(String, TradesLocal),

    // message from Bulk computation
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    // fourth: market reference on which these trades were computed.
    BulkReceive((TradesLocal, PmPortfolio, TradesLocal, String)),
    BulkBusy, // unable to compute right now, as it's busy

    // message from the processor above.
    // elements:
    //    1st elt: trades for which portfolio was computed.
    //    2nd elt: portfolio:
    //    3rd market for which it was computed.
    //    4th actor where this was sent from.
    NewTradePortfolio(
        (
            TradesLocal,
            PmPortfolio,
            String,
            ActorRef<ProcessorMiddleMessage<MT>>,
        ),
    ),
    // processing stat:
    //   1st arg: processor name
    //   2nd arg: when the events ocurred.
    //   3rd arg: cumulative number of trades processed.
    ProcessingStat((String, chrono::NaiveDateTime, usize)),
    Metric(Vec<PricingMetric>), // we compute the vector of pricing metrics.
}

impl<MT> ProcessorMiddleMessage<MT> {
    pub(crate) fn get_trade(&self) -> Option<String> {
        match self {
            ProcessorMiddleMessage::NewTrade(trade_id) => Some(trade_id.clone()),
            _ => None,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum ProcessorBulkMessage<MT> {
    // is a triple
    //    first is the market type, a name of the market
    //    second - is a vector of trades that need to be computed.
    //    third - an actor processing ProcessorMiddleMessage
    //    4th: vector of pricing metrics to compute
    NewBulk(
        (
            String,
            TradesLocal,
            ActorRef<ProcessorMiddleMessage<MT>>,
            Vec<PricingMetric>,
        ),
    ),
    Abandon, // TODO: WHAT TO DO W/ THIS???
}

// Accounting for the distribution of messages of types
//   ProcessorMiddleMessage

#[derive(Debug)]
pub(crate) struct PNStateDistr(DashMap<ProcessorMiddleMessageStates, u64>);

ref_deref_trait!(PNStateDistr, DashMap<ProcessorMiddleMessageStates, u64>);

impl PNStateDistr {
    pub(crate) fn new() -> Self {
        PNStateDistr::from([
            (ProcessorMiddleMessageStates::NewTrade, 0),
            (ProcessorMiddleMessageStates::NewMarket, 0),
            (ProcessorMiddleMessageStates::Behind, 0),
            (ProcessorMiddleMessageStates::BulkReceive, 0),
            (ProcessorMiddleMessageStates::BulkBusy, 0),
            (ProcessorMiddleMessageStates::NewTradePortfolio, 0),
            (ProcessorMiddleMessageStates::ProcessingStat, 0),
            (ProcessorMiddleMessageStates::Metric, 0),
        ])
    }
}

impl PNStateDistr {
    // increment one of the states by 1. used for accounting.
    pub(crate) fn incr_one(&self, ps: ProcessorMiddleMessageStates) {
        self.entry(ps).and_modify(|count| *count += 1).or_insert(1);
    }
}

impl<const N: usize> From<[(ProcessorMiddleMessageStates, u64); N]> for PNStateDistr {
    fn from(arr: [(ProcessorMiddleMessageStates, u64); N]) -> Self {
        let dm = DashMap::new();
        for (pmm, pmm_freq) in arr.iter() {
            dm.insert(*pmm, *pmm_freq);
        }
        Self(dm)
    }
}
