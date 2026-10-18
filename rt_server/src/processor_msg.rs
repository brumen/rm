use dashmap::DashMap;
/// messages for the Multiple Actor references.
use ractor::ActorRef;
use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use strum::{AsRefStr, EnumDiscriminants, IntoStaticStr};

use crate::all_markets::MarketLease;
use crate::portfolio::PmPortfolio;
use crate::pricer::PricingMetric;
use crate::ref_deref_trait;

pub(crate) type TradesLocal = HashSet<String>;
pub(crate) type PortfolioUpdate<MT> = (
    TradesLocal,
    PmPortfolio,
    MarketLease,
    ActorRef<ProcessorMiddleMessage<MT>>,
);
pub(crate) type BehindUpdate = (MarketLease, TradesLocal);

/// One pending update per processor link. Actor mailboxes carry a notification
/// referencing this slot, so superseded portfolios and their market leases drop
/// immediately instead of accumulating behind a slow receiver.
#[derive(Debug)]
pub(crate) struct Latest<T> {
    value: Mutex<Option<T>>,
}

impl<T> Default for Latest<T> {
    fn default() -> Self {
        Self {
            value: Mutex::new(None),
        }
    }
}

impl<T> Latest<T> {
    pub(crate) fn replace(&self, value: T) -> bool {
        let previous = self.value.lock().unwrap().replace(value);
        previous.is_none()
    }

    fn merge(&self, mut value: T, merge: impl FnOnce(&mut T, T)) -> bool {
        let mut slot = self.value.lock().unwrap();
        let notify = slot.is_none();
        if let Some(previous) = slot.take() {
            merge(&mut value, previous);
        }
        *slot = Some(value);
        notify
    }

    pub(crate) fn take(&self) -> Option<T> {
        self.value.lock().unwrap().take()
    }
}

pub(crate) fn send_latest_market(
    recipient: &ActorRef<ProcessorMiddleMessage<String>>,
    pending: &Arc<Latest<String>>,
) -> Result<(), ractor::MessagingErr<ProcessorMiddleMessage<String>>> {
    if pending.replace("future".to_string()) {
        if let Err(error) =
            recipient.send_message(ProcessorMiddleMessage::NewMarket(pending.clone()))
        {
            pending.take();
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) fn send_latest_portfolio(
    recipient: &ActorRef<ProcessorMiddleMessage<String>>,
    pending: &Arc<Latest<PortfolioUpdate<String>>>,
    update: PortfolioUpdate<String>,
) -> Result<(), ractor::MessagingErr<ProcessorMiddleMessage<String>>> {
    if pending.replace(update) {
        if let Err(error) =
            recipient.send_message(ProcessorMiddleMessage::NewTradePortfolio(pending.clone()))
        {
            pending.take();
            return Err(error);
        }
    }
    Ok(())
}

pub(crate) fn send_latest_behind(
    recipient: &ActorRef<ProcessorMiddleMessage<String>>,
    pending: &Arc<Latest<BehindUpdate>>,
    update: BehindUpdate,
) -> Result<(), ractor::MessagingErr<ProcessorMiddleMessage<String>>> {
    // Keep every missing trade request, while retaining only the newest market.
    if pending.merge(update, |latest, previous| latest.1.extend(previous.1)) {
        if let Err(error) = recipient.send_message(ProcessorMiddleMessage::Behind(pending.clone()))
        {
            pending.take();
            return Err(error);
        }
    }
    Ok(())
}

/// message that the new processor receives
#[derive(EnumDiscriminants)]
#[strum_discriminants(name(ProcessorMiddleMessageStates))] // Renames the generated enum
#[strum_discriminants(derive(std::hash::Hash))] // Adds hash trait to ProcessorMiddleMessageStates
#[strum_discriminants(derive(strum::Display))] // Adds display trait to ProcessorMiddleMessageStates
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(dead_code)]
#[derive(Clone, Debug, AsRefStr)]
pub(crate) enum ProcessorMiddleMessage<MT> {
    NewTrade(String),           // message from trade producer, trade id.
    NewMarket(Arc<Latest<MT>>), // latest notification from the market handler
    // message from Processor_below:
    //    1st arg: market that we are evaluating
    //    2nd arg: missing trades that we still need -
    //                if Vec.is_empty() then we accepted the portfolio, otherwise market is not accepted.
    Behind(Arc<Latest<BehindUpdate>>),

    // message from Bulk computation
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    // fourth: lease retaining the market on which these trades were computed.
    BulkReceive((TradesLocal, PmPortfolio, TradesLocal, MarketLease)),
    BulkBusy, // unable to compute right now, as it's busy

    // message from the processor above.
    // elements:
    //    1st elt: trades for which portfolio was computed.
    //    2nd elt: portfolio:
    //    3rd lease retaining the market for which it was computed.
    //    4th actor where this was sent from.
    NewTradePortfolio(Arc<Latest<PortfolioUpdate<MT>>>),
    // processing stat:
    //   1st arg: processor name
    //   2nd arg: when the events ocurred.
    //   3rd arg: cumulative number of trades processed.
    ProcessingStat((String, chrono::NaiveDateTime, usize)),
    Metric(Vec<PricingMetric>), // we compute the vector of pricing metrics.
}

// impl<MT> ProcessorMiddleMessage<MT> {
//     pub(crate) fn get_trade(&self) -> Option<String> {
//         match self {
//             ProcessorMiddleMessage::NewTrade(trade_id) => Some(trade_id.clone()),
//             _ => None,
//         }
//     }
// }

#[allow(dead_code)]
#[derive(Clone, Debug, AsRefStr)]
pub enum ProcessorBulkMessage<MT> {
    // is a triple
    //    first is a lease retaining the market
    //    second - is a vector of trades that need to be computed.
    //    third - an actor processing ProcessorMiddleMessage
    //    4th: vector of pricing metrics to compute
    NewBulk(
        (
            MarketLease,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::all_markets::AllMarkets;
    use crate::markets::letf_market::LETFMarketType;
    use ractor::{async_trait, Actor, ActorProcessingErr};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot, Notify};

    enum Observed {
        Portfolio(String),
        Behind(String, TradesLocal),
        Market(String),
    }

    struct SlowReceiver;
    struct ReceiverState {
        entered: Option<oneshot::Sender<()>>,
        resume: Arc<Notify>,
        observed: mpsc::UnboundedSender<Observed>,
    }

    #[async_trait]
    impl Actor for SlowReceiver {
        type Msg = ProcessorMiddleMessage<String>;
        type State = ReceiverState;
        type Arguments = ReceiverState;

        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            state: Self::Arguments,
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(state)
        }

        async fn handle(
            &self,
            _: ActorRef<Self::Msg>,
            message: Self::Msg,
            state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match message {
                ProcessorMiddleMessage::NewTradePortfolio(pending) => {
                    if let Some(update) = pending.take() {
                        let name = update.2.name().clone();
                        if let Some(entered) = state.entered.take() {
                            entered.send(()).unwrap();
                            // Hold this market while the producer runs ahead.
                            state.resume.notified().await;
                        }
                        drop(update);
                        state.observed.send(Observed::Portfolio(name))?;
                    }
                }
                ProcessorMiddleMessage::Behind(pending) => {
                    if let Some((lease, trades)) = pending.take() {
                        let name = lease.name().clone();
                        drop(lease);
                        state.observed.send(Observed::Behind(name, trades))?;
                    }
                }
                ProcessorMiddleMessage::NewMarket(pending) => {
                    if let Some(name) = pending.take() {
                        state.observed.send(Observed::Market(name))?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_stalled_receiver_keeps_only_active_and_latest_pending_markets() {
        let all = AllMarkets::new();
        let portfolio_outbox = Arc::new(Latest::default());
        let market_outbox = Arc::new(Latest::default());
        let feedback_outbox = Arc::new(Latest::default());
        let (entered_tx, entered_rx) = oneshot::channel();
        let resume = Arc::new(Notify::new());
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
        let (receiver, task) = Actor::spawn(
            None,
            SlowReceiver,
            ReceiverState {
                entered: Some(entered_tx),
                resume: resume.clone(),
                observed: observed_tx,
            },
        )
        .await
        .unwrap();

        all.insert_both(
            "curr".into(),
            "active".into(),
            Arc::new(LETFMarketType::new("active".into())),
        )
        .await;
        let first = Arc::new(LETFMarketType::new("inflight".into()));
        let first_weak = Arc::downgrade(&first);
        all.insert_both("new".into(), "inflight".into(), first)
            .await;
        send_latest_portfolio(
            &receiver,
            &portfolio_outbox,
            (
                TradesLocal::new(),
                PmPortfolio::new(),
                all.lease(&"inflight".into()).await.unwrap(),
                receiver.clone(),
            ),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();

        let mut snapshots = Vec::new();
        for i in 0..1000 {
            let name = format!("market-{i}");
            let market = Arc::new(LETFMarketType::new(name.clone()));
            snapshots.push(Arc::downgrade(&market));
            all.insert_both("new".into(), name.clone(), market.clone())
                .await;
            let lease = all.lease(&name).await.unwrap();
            send_latest_portfolio(
                &receiver,
                &portfolio_outbox,
                (
                    TradesLocal::new(),
                    PmPortfolio::new(),
                    lease.clone(),
                    receiver.clone(),
                ),
            )
            .unwrap();
            send_latest_behind(
                &receiver,
                &feedback_outbox,
                (lease, TradesLocal::from([format!("trade-{i}")])),
            )
            .unwrap();
            // Same operation used for every tick by MarketProducer.
            all.update_future(market).await;
            send_latest_market(&receiver, &market_outbox).unwrap();
            assert_eq!(all.list_market_names().await.len(), 4);
        }
        assert!(
            first_weak.upgrade().is_some(),
            "an active handler lost its market"
        );
        assert_eq!(
            snapshots
                .iter()
                .filter(|weak| weak.upgrade().is_some())
                .count(),
            1
        );
        all.remove_processor("new").await;
        assert!(
            all.get(&"market-999".into()).await.is_some(),
            "pending handoff lost its market"
        );

        resume.notify_one();
        let mut portfolios = Vec::new();
        let mut feedback = Vec::new();
        let mut market_notifications = Vec::new();
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_secs(2), observed_rx.recv())
                .await
                .unwrap()
                .unwrap()
            {
                Observed::Portfolio(name) => portfolios.push(name),
                Observed::Behind(name, trades) => feedback.push((name, trades)),
                Observed::Market(name) => market_notifications.push(name),
            }
        }
        assert_eq!(market_notifications, ["future"]);
        assert_eq!(portfolios, ["inflight", "market-999"]);
        assert_eq!(feedback.len(), 1);
        assert_eq!(feedback[0].0, "market-999");
        assert_eq!(
            feedback[0].1.len(),
            1000,
            "coalescing lost missing trade requests"
        );
        assert!(
            observed_rx.try_recv().is_err(),
            "superseded updates were queued separately"
        );

        // Cleanup must continue even when processors stop changing markets.
        all.update_future(Arc::new(LETFMarketType::new("latest".into())))
            .await;
        assert_eq!(all.list_market_names().await, ["active", "future"]);
        assert!(first_weak.upgrade().is_none());
        assert!(snapshots.iter().all(|weak| weak.upgrade().is_none()));
        receiver.stop(None);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn test_failed_notifications_release_pending_leases() {
        let all = AllMarkets::new();
        let (observed_tx, _) = mpsc::unbounded_channel();
        let (receiver, task) = Actor::spawn(
            None,
            SlowReceiver,
            ReceiverState {
                entered: None,
                resume: Arc::new(Notify::new()),
                observed: observed_tx,
            },
        )
        .await
        .unwrap();
        receiver.stop(None);
        task.await.unwrap();
        let market_outbox = Arc::new(Latest::default());
        assert!(send_latest_market(&receiver, &market_outbox).is_err());
        assert!(market_outbox.take().is_none());
        let market = Arc::new(LETFMarketType::new("unused".into()));
        let weak = Arc::downgrade(&market);
        all.insert_both("new".into(), "unused".into(), market).await;
        let portfolio_outbox = Arc::new(Latest::default());
        let feedback_outbox = Arc::new(Latest::default());
        assert!(send_latest_portfolio(
            &receiver,
            &portfolio_outbox,
            (
                TradesLocal::new(),
                PmPortfolio::new(),
                all.lease(&"unused".into()).await.unwrap(),
                receiver.clone(),
            )
        )
        .is_err());
        assert!(send_latest_behind(
            &receiver,
            &feedback_outbox,
            (
                all.lease(&"unused".into()).await.unwrap(),
                TradesLocal::new()
            )
        )
        .is_err());
        assert!(portfolio_outbox.take().is_none());
        assert!(feedback_outbox.take().is_none());
        all.remove_processor("new").await;
        assert!(weak.upgrade().is_none());
    }
}
