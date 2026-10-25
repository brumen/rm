//! Internal, versioned actor protocol. Kafka payloads are unchanged.
use crate::portfolio::PmPortfolio;
use crate::pricer::PricingMetric;
use crate::ref_deref_trait;
use dashmap::DashMap;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use strum::{AsRefStr, EnumDiscriminants, IntoStaticStr};
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Instant;

#[derive(Debug)]
pub(crate) struct TradeEvent<T> {
    pub id: String,
    pub version: u64,
    pub value: T,
    pub received: Instant,
}

#[derive(Debug)]
pub(crate) struct TradeDelivery<T> {
    pub event: Arc<TradeEvent<T>>,
    // The permit follows the event through the mailbox, pending queue and worker.
    // Returning from handle() alone does not release admission capacity.
    pub _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct PricedTrade<T> {
    pub event: Arc<TradeEvent<T>>,
    pub results: PmPortfolio,
}

#[derive(Debug)]
pub(crate) struct PortfolioUpdate<T, MT> {
    pub sequence: u64,
    pub market: Arc<MT>,
    pub metrics: Vec<PricingMetric>,
    pub trades: HashMap<String, Arc<PricedTrade<T>>>,
    pub failed: HashMap<String, u64>,
}

#[derive(Debug)]
pub(crate) struct PortfolioDelivery<T, MT> {
    pub update: PortfolioUpdate<T, MT>,
    pub _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct PricingCompletion<T> {
    pub epoch: u64,
    pub outcomes: Vec<(Arc<TradeEvent<T>>, Result<PmPortfolio, String>)>,
    pub elapsed: std::time::Duration,
}

#[derive(EnumDiscriminants, Debug, AsRefStr)]
#[strum_discriminants(name(ProcessorMiddleMessageStates))]
#[strum_discriminants(derive(std::hash::Hash, strum::Display, IntoStaticStr))]
pub(crate) enum ProcessorMessage<T, MT> {
    NewTrade(TradeDelivery<T>),
    NewMarket,
    NewTradePortfolio(PortfolioDelivery<T, MT>),
    Metric(Vec<PricingMetric>),
    Wake,
    PricingDone(PricingCompletion<T>),
    PublishDone {
        sequence: u64,
        elapsed: std::time::Duration,
        result: Result<(), String>,
    },
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
            (ProcessorMiddleMessageStates::NewTradePortfolio, 0),
            (ProcessorMiddleMessageStates::Metric, 0),
            (ProcessorMiddleMessageStates::Wake, 0),
            (ProcessorMiddleMessageStates::PricingDone, 0),
            (ProcessorMiddleMessageStates::PublishDone, 0),
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
