//! Shared actor scheduler. Actor handlers only change state and launch bounded
//! work; they never sleep, price trades, await admission, or await Kafka.
use futures::FutureExt;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;
use crate::portfolio::PmPortfolio;
use crate::portfolio_results::PortfolioReporter;
use crate::pricer::{PriceTrade, PricingMetric};
use crate::processor_bulk::{price_batch, PricingStyle};
use crate::processor_curr::{RTOperatingMode, SharedPublisher, NTP_ALLOW_BEHIND};
use crate::processor_msg::*;
use crate::throttle::{AdaptiveThrottle, ThrottleConfig};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Role {
    New,
    Middle,
    Current,
}

pub(crate) struct Endpoint<T, MT> {
    pub actor: ActorRef<ProcessorMessage<T, MT>>,
    pub admission: Arc<Semaphore>,
    pub snapshots: Arc<Semaphore>,
    pub market_slot: Arc<Mutex<Option<Arc<MT>>>>,
}
impl<T, MT> Clone for Endpoint<T, MT> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            admission: self.admission.clone(),
            snapshots: self.snapshots.clone(),
            market_slot: self.market_slot.clone(),
        }
    }
}
impl<T: Send + Sync + 'static, MT: Send + Sync + 'static> Endpoint<T, MT> {
    /// Admit an immutable trade version before sending it to the actor mailbox.
    ///
    /// Waiting here applies backpressure to ingress. The delivery retains its
    /// permit until pricing, a covering snapshot, or an explicit failure resolves
    /// that version; receiving the message alone does not free capacity.
    pub async fn send_trade(&self, event: Arc<TradeEvent<T>>) -> Result<(), String> {
        let permit = self
            .admission
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| e.to_string())?;
        self.actor
            .send_message(ProcessorMessage::NewTrade(TradeDelivery {
                event,
                _permit: permit,
            }))
            .map_err(|_| "Trade receiver stopped".to_string())
    }
    /// Offer the newest complete market to the New processor without queueing
    /// every intermediate snapshot. Replacing an occupied slot needs no extra
    /// notification: the pending `NewMarket` message will read the latest value.
    pub fn market(&self, market: Arc<MT>) -> Result<(), String> {
        if self.actor.get_status() >= ractor::ActorStatus::Draining {
            return Err("Market receiver stopped".into());
        }
        let notify = self.market_slot.lock().unwrap().replace(market).is_none();
        if notify {
            self.actor
                .send_message(ProcessorMessage::NewMarket)
                .map_err(|_| "Market receiver stopped".to_string())?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Task(pub JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct Processor<T, MT: Debug> {
    pub processor_name: String,
    pub role: Role,
    pub below: Option<Endpoint<T, MT>>,
    pub publisher: Option<SharedPublisher>,
    pub operating_mode: RTOperatingMode,
    pub all_markets: Arc<AllMarkets<Arc<MT>>>,
    pub reporter: Option<PortfolioReporter>,
    pub state_distr: Arc<PNStateDistr>,
    pub pricing_mode: PricingStyle,
    pub config: ThrottleConfig,
    pub admission: Arc<Semaphore>,
    pub snapshots: Arc<Semaphore>,
    pub market_slot: Arc<Mutex<Option<Arc<MT>>>>,
}

impl<T, MT: Debug> Drop for Processor<T, MT> {
    fn drop(&mut self) {
        self.admission.close();
        self.snapshots.close();
    }
}

pub(crate) struct ProcessorState<T, MT> {
    pending: VecDeque<TradeDelivery<T>>,
    known: HashMap<String, Arc<TradeEvent<T>>>,
    priced: HashMap<String, Arc<PricedTrade<T>>>,
    failed: HashMap<String, u64>,
    local_ahead: HashSet<String>,
    failures: u64,
    portfolio: PmPortfolio,
    metrics: Vec<PricingMetric>,
    market: Option<Arc<MT>>,
    requested_market: Option<Arc<MT>>,
    repair: VecDeque<Arc<TradeEvent<T>>>,
    refresh: bool,
    refreshing: bool,
    epoch: u64,
    worker: Option<Task>,
    timer: Option<Task>,
    publishing: Option<Task>,
    publish_sequence: u64,
    publish_retry: Instant,
    publish_failures: u32,
    dirty: bool,
    upstream_sequence: u64,
    output_sequence: u64,
    throttle: AdaptiveThrottle,
    stalled_logged: bool,
    report_at: Instant,
    report_pending: bool,
}

impl<T, MT> ProcessorState<T, MT> {
    /// Check whether successful local pricing covers this trade version or a
    /// newer replacement. Known input and failed attempts do not count as coverage.
    fn covered(&self, event: &TradeEvent<T>) -> bool {
        self.priced
            .get(&event.id)
            .is_some_and(|p| p.event.version >= event.version)
    }
    /// Check whether a delivery can release admission capacity, either because
    /// it is covered or because this version has been explicitly marked failed.
    /// Failure preserves the latest known trade for retry on a later market or
    /// metric set.
    fn completed(&self, event: &TradeEvent<T>) -> bool {
        self.covered(event)
            || self
                .failed
                .get(&event.id)
                .is_some_and(|v| *v >= event.version)
    }
    /// Remove resolved deliveries and repairs. Dropping a trade delivery returns
    /// its admission permit to ingress; this acknowledges processing, not Kafka
    /// publication of the resulting portfolio.
    fn prune_pending(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        self.pending = pending
            .into_iter()
            .filter(|p| !self.completed(&p.event))
            .collect();
        let repair = std::mem::take(&mut self.repair);
        self.repair = repair.into_iter().filter(|p| !self.completed(p)).collect();
    }
    /// Apply one priced contribution while protecting newer installed versions.
    /// Replacing a position subtracts its previous contribution before adding
    /// the new one, then updates the cached lead and marks output as changed.
    fn install(&mut self, priced: Arc<PricedTrade<T>>) {
        if self
            .priced
            .get(&priced.event.id)
            .is_some_and(|old| old.event.version > priced.event.version)
        {
            return;
        }
        if let Some(old) = self.priced.insert(priced.event.id.clone(), priced.clone()) {
            for (metric, contribution) in old.results.iter() {
                if let Some(total) = self.portfolio.get_mut(metric) {
                    *total -= contribution.clone();
                }
            }
        }
        for (metric, contribution) in priced.results.iter() {
            self.portfolio.assign_metric(metric, contribution.clone());
        }
        if self
            .throttle
            .covered_upstream(&priced.event.id, priced.event.version)
        {
            self.local_ahead.remove(&priced.event.id);
        } else {
            self.local_ahead.insert(priced.event.id.clone());
        }
        self.dirty = true;
    }
}

impl<T, MT> Processor<T, MT>
where
    T: Clone + Debug + Send + Sync + PriceTrade<MT> + 'static,
    MT: MarketTypeT + Debug + Send + Sync + 'static,
    MT::MP: Clone,
{
    /// Capture the current portfolio and pacing statistics for diagnostics.
    /// The New role reports no upstream lead or stall because it is the source
    /// of portfolio updates. `pump` coalesces these captures to limit copying.
    fn report(&self, state: &ProcessorState<T, MT>) {
        if let Some(reporter) = &self.reporter {
            let lead = if self.role == Role::New {
                0
            } else {
                state.local_ahead.len()
            };
            let mut throttle = state.throttle.snapshot(
                state.pending.len(),
                state.pending.front().map(|p| p.event.received),
                lead,
                state.failures,
                Instant::now(),
            );
            if self.role == Role::New {
                throttle.upstream_stalled = false;
            }
            reporter.record_with_throttle(
                state.market.as_ref().map(|m| m.market_name()).as_deref(),
                if state.refreshing {
                    "CalculatingBulk"
                } else if state.market.is_none() {
                    "Waiting for results"
                } else {
                    "Current"
                },
                &state.portfolio,
                Some(throttle),
            );
        }
    }
    /// Arrange one future `Wake` message without sleeping inside the handler.
    /// An existing timer is retained so repeated input cannot create a growing
    /// collection of timer tasks. Handling `Wake` clears the slot for reuse.
    fn schedule(
        &self,
        myself: &ActorRef<ProcessorMessage<T, MT>>,
        state: &mut ProcessorState<T, MT>,
        delay: Duration,
    ) {
        if state.timer.is_some() {
            return;
        }
        let actor = myself.clone();
        // No instrumented handler span is propagated around an endless self-message chain.
        state.timer = Some(Task(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = actor.send_message(ProcessorMessage::Wake);
        })));
    }
    /// Start a pricing job using the current market, metrics and epoch.
    ///
    /// Callers ensure the worker slot is free. The task prices immutable event
    /// values outside the actor handler and returns a `PricingDone` message.
    /// Timeouts and panics become failed outcomes; the handler checks the epoch
    /// before applying results so superseded work cannot overwrite newer state.
    fn launch_pricing(
        &self,
        myself: &ActorRef<ProcessorMessage<T, MT>>,
        state: &mut ProcessorState<T, MT>,
        events: Vec<Arc<TradeEvent<T>>>,
    ) {
        let Some(market) = state.market.clone() else {
            return;
        };
        let metrics = state.metrics.clone();
        let style = self.pricing_mode.clone();
        let epoch = state.epoch;
        let timeout = self.config.worker_timeout;
        let actor = myself.clone();
        state.worker = Some(Task(tokio::spawn(async move {
            let start = Instant::now();
            let fallback = events.clone();
            let job = std::panic::AssertUnwindSafe(price_batch(events, metrics, market, style))
                .catch_unwind();
            let outcomes = match tokio::time::timeout(timeout, job).await {
                Ok(Ok(outcomes)) => outcomes,
                result => {
                    let reason = if result.is_err() {
                        "Pricing timed out"
                    } else {
                        "Pricing worker panicked"
                    };
                    fallback
                        .into_iter()
                        .map(|e| (e, Err(reason.to_string())))
                        .collect()
                }
            };
            let _ = actor.send_message(ProcessorMessage::PricingDone(PricingCompletion {
                epoch,
                outcomes,
                elapsed: start.elapsed(),
            }));
        })));
    }
    /// Evaluate an upstream portfolio for the Middle or Current role.
    ///
    /// Ordered snapshots with matching metrics update the upstream performance
    /// estimate even when Current rejects one for missing ten or more locally
    /// priced versions. Accepting a snapshot cancels obsolete pricing, adopts
    /// its market and results, and queues missing local contributions for repair.
    fn accept_snapshot(&self, state: &mut ProcessorState<T, MT>, update: PortfolioUpdate<T, MT>) {
        if update.sequence <= state.upstream_sequence {
            return;
        }
        state.upstream_sequence = update.sequence;
        if update.metrics != state.metrics || state.metrics.is_empty() {
            return;
        }
        // Measure all comparable portfolios, including rejected ones.
        state.throttle.observe_upstream(
            update
                .trades
                .iter()
                .map(|(id, p)| (id.clone(), p.event.version))
                .collect(),
            Instant::now(),
        );
        let missing: Vec<_> = state
            .priced
            .iter()
            .filter(|(id, p)| {
                update
                    .trades
                    .get(*id)
                    .is_none_or(|new| new.event.version < p.event.version)
            })
            .map(|(_, p)| p.event.clone())
            .collect();
        state.local_ahead = missing.iter().map(|e| e.id.clone()).collect();
        if self.role == Role::Current && missing.len() >= NTP_ALLOW_BEHIND {
            debug!(processor=%self.processor_name, missing=missing.len(), "Portfolio rejected; adaptive pacing will let upstream catch up");
            return;
        }
        // Cancel obsolete work. Its input events remain pending until a matching
        // result or a newer successful snapshot covers them.
        let market_changed = state
            .market
            .as_ref()
            .is_none_or(|m| m.market_name() != update.market.market_name());
        let retry_ids: Vec<_> = if market_changed {
            state.failed.keys().cloned().collect()
        } else {
            vec![]
        };
        state.worker = None;
        state.refreshing = false;
        state.refresh = false;
        state.epoch += 1;
        state.market = Some(update.market.clone());
        state.failed = update.failed;
        state.repair = missing.into();
        state.priced.clear();
        state.local_ahead.clear();
        state.portfolio = PmPortfolio::new();
        for (_, priced) in update.trades {
            let event = &priced.event;
            if state
                .known
                .get(&event.id)
                .is_none_or(|known| known.version < event.version)
            {
                state.known.insert(event.id.clone(), event.clone());
            }
            state.install(priced);
        }
        // Retry versions acknowledged as failed on the previous market. Ordinary
        // pending deliveries stay paced; they are not reconciliation repairs.
        if !(self.role == Role::Current && self.operating_mode == RTOperatingMode::SingleBuffer) {
            let mut repair_ids: HashSet<_> = state
                .repair
                .iter()
                .map(|e| (e.id.clone(), e.version))
                .collect();
            for id in retry_ids {
                let Some(event) = state.known.get(&id) else {
                    continue;
                };
                if !state.completed(event) && repair_ids.insert((event.id.clone(), event.version)) {
                    state.repair.push_back(event.clone());
                }
            }
        }
        state.dirty = true;
        // Retain the Arc in state; workers and queued snapshots do not depend on
        // a later name lookup surviving registry pruning.
        self.all_markets.insert_both(
            self.processor_name.clone(),
            update.market.market_name(),
            update.market,
        );
        state.prune_pending();
    }
    /// Send changed state once bulk pricing and reconciliation have finished.
    ///
    /// New and Middle try to acquire the downstream snapshot permit without
    /// waiting in the handler. Current launches one Kafka publication worker;
    /// changes made while it runs remain pending in actor state. `PublishDone`
    /// schedules failures for retry using the latest portfolio.
    fn flush_output(
        &self,
        myself: &ActorRef<ProcessorMessage<T, MT>>,
        state: &mut ProcessorState<T, MT>,
    ) {
        if !state.dirty || state.refreshing || !state.repair.is_empty() {
            return;
        }
        let Some(market) = state.market.clone() else {
            return;
        };
        if let Some(below) = &self.below {
            if let Ok(permit) = below.snapshots.clone().try_acquire_owned() {
                state.output_sequence += 1;
                let update = PortfolioUpdate {
                    sequence: state.output_sequence,
                    market,
                    metrics: state.metrics.clone(),
                    trades: state.priced.clone(),
                    failed: state.failed.clone(),
                };
                if below
                    .actor
                    .send_message(ProcessorMessage::NewTradePortfolio(PortfolioDelivery {
                        update,
                        _permit: permit,
                    }))
                    .is_ok()
                {
                    state.dirty = false;
                } else {
                    // Stop rather than silently accumulating data for a dead sink.
                    error!(processor=%self.processor_name, "Downstream processor stopped");
                    myself.stop(Some("Downstream processor stopped".into()));
                }
            }
        } else if let Some(publisher) = self.publisher.clone() {
            if state.publishing.is_some() || Instant::now() < state.publish_retry {
                return;
            }
            state.publish_sequence += 1;
            let sequence = state.publish_sequence;
            let portfolio = state.portfolio.clone();
            let actor = myself.clone();
            let timeout = self.config.worker_timeout;
            state.dirty = false;
            state.publishing = Some(Task(tokio::spawn(async move {
                let start = Instant::now();
                let job = std::panic::AssertUnwindSafe(publisher.publish(portfolio)).catch_unwind();
                let result = match tokio::time::timeout(timeout, job).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => Err("Publisher panicked".into()),
                    Err(_) => Err("Publisher timed out".into()),
                };
                let _ = actor.send_message(ProcessorMessage::PublishDone {
                    sequence,
                    elapsed: start.elapsed(),
                    result,
                });
            })));
        } else {
            state.dirty = false;
        }
    }
    /// Advance the shared processor state machine after a message or timer wake.
    ///
    /// First release resolved deliveries and try to flush output, then launch at
    /// most one pricing job. For New, a pending market triggers full repricing of
    /// the latest known trades. Otherwise, repairs precede incremental trade work.
    /// Incremental New work uses local capacity limits; ordinary Middle/Current
    /// work also follows upstream lead. Accepted-portfolio repairs bypass that
    /// lead restriction so reconciliation can finish. Finally, schedule any
    /// needed wake and diagnostic capture while keeping the handler responsive.
    fn pump(&self, myself: &ActorRef<ProcessorMessage<T, MT>>, state: &mut ProcessorState<T, MT>) {
        state.prune_pending();
        let now = Instant::now();
        state
            .throttle
            .set_busy(!state.pending.is_empty() || !state.repair.is_empty(), now);
        let stalled = state.throttle.stalled(now);
        if stalled && !state.stalled_logged && self.role != Role::New {
            warn!(processor=%self.processor_name, pending=state.pending.len(), "Upstream progress stalled; using conservative fallback pacing");
        }
        state.stalled_logged = stalled;
        self.flush_output(myself, state);
        if state.worker.is_none() && !state.metrics.is_empty() {
            // Do not create another full snapshot while the previous one is
            // waiting for delivery. Incoming control messages remain serviceable.
            let output_blocked = state.dirty
                && state.market.is_some()
                && state.repair.is_empty()
                && !state.refreshing
                && !state.refresh
                && (self.below.is_some()
                    || state.publishing.is_some()
                    || now < state.publish_retry);
            if !output_blocked {
                if state.refresh || (self.role == Role::New && state.requested_market.is_some()) {
                    if let Some(market) = state.requested_market.take() {
                        state.market = Some(market.clone());
                        self.all_markets.insert_both(
                            self.processor_name.clone(),
                            market.market_name(),
                            market,
                        );
                    }
                    if state.market.is_some() {
                        state.epoch += 1;
                        state.priced.clear();
                        state.local_ahead.clear();
                        state.failed.clear();
                        state.portfolio = PmPortfolio::new();
                        state.refresh = false;
                        state.refreshing = true;
                        state.repair.clear();
                        let events = state.known.values().cloned().collect::<Vec<_>>();
                        if events.is_empty() {
                            state.refreshing = false;
                            state.dirty = true;
                        } else {
                            self.launch_pricing(myself, state, events);
                        }
                    }
                } else if state.market.is_some() && state.repair.len() > self.config.burst {
                    // Reconciliation of a large portfolio is one bounded worker
                    // job, not repeated scans of the entire repair population.
                    let events = state.repair.drain(..).collect();
                    state.refreshing = true;
                    self.launch_pricing(myself, state, events);
                } else if state.market.is_some()
                    && (!state.repair.is_empty()
                        || !(self.role == Role::Current
                            && self.operating_mode == RTOperatingMode::SingleBuffer))
                {
                    let mut seen = HashSet::new();
                    let candidates = state
                        .repair
                        .iter()
                        .chain(state.pending.iter().map(|p| &p.event))
                        .filter(|e| !state.completed(e) && seen.insert((e.id.clone(), e.version)))
                        .take(self.config.burst)
                        .cloned()
                        .collect::<Vec<_>>();
                    let lead = state.local_ahead.len();
                    // Repairs restore previously priced local contributions on
                    // the accepted market; withholding them delays publication
                    // rather than letting upstream catch up.
                    let paced = self.role != Role::New && state.repair.is_empty();
                    let count = state.throttle.take(candidates.len(), lead, paced, now);
                    if count > 0 {
                        self.launch_pricing(
                            myself,
                            state,
                            candidates.into_iter().take(count).collect(),
                        );
                    }
                }
            }
        }
        let active = !state.pending.is_empty()
            || !state.repair.is_empty()
            || state.dirty
            || state.requested_market.is_some()
            || state.refresh;
        if active {
            let delay = if state.market.is_none()
                || state.metrics.is_empty()
                || state.worker.is_some()
                || (state.publishing.is_some() && state.dirty)
                || (state.local_ahead.len() >= 8 && !stalled && self.role != Role::New)
            {
                Duration::from_millis(50)
            } else {
                state.throttle.wake_delay()
            };
            self.schedule(myself, state, delay);
        }
        if active {
            state.report_pending = true;
        }
        if state.report_pending && now >= state.report_at {
            self.report(state);
            state.report_pending = false;
            state.report_at = now + Duration::from_millis(100);
        }
        if state.report_pending && !active {
            self.schedule(
                myself,
                state,
                state
                    .report_at
                    .saturating_duration_since(now)
                    .max(Duration::from_millis(1)),
            );
        }
    }
}

#[async_trait]
impl<T, MT> Actor for Processor<T, MT>
where
    T: Clone + Debug + Send + Sync + PriceTrade<MT> + 'static,
    MT: MarketTypeT + Debug + Send + Sync + 'static,
    MT::MP: Clone,
{
    type Msg = ProcessorMessage<T, MT>;
    type State = ProcessorState<T, MT>;
    type Arguments = ();
    /// Initialize empty portfolio state, worker slots and performance estimates.
    /// Pricing becomes eligible after metrics and a market have been supplied.
    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        _: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        info!(processor=%self.processor_name, role=?self.role, capacity=self.config.capacity, "Starting adaptive processor");
        let now = Instant::now();
        Ok(ProcessorState {
            pending: VecDeque::new(),
            known: HashMap::new(),
            priced: HashMap::new(),
            failed: HashMap::new(),
            local_ahead: HashSet::new(),
            failures: 0,
            portfolio: PmPortfolio::new(),
            metrics: vec![],
            market: None,
            requested_market: None,
            repair: VecDeque::new(),
            refresh: false,
            refreshing: false,
            epoch: 0,
            worker: None,
            timer: None,
            publishing: None,
            publish_sequence: 0,
            publish_retry: now,
            publish_failures: 0,
            dirty: false,
            upstream_sequence: 0,
            output_sequence: 0,
            throttle: AdaptiveThrottle::new(self.config.clone(), now),
            stalled_logged: false,
            report_at: now,
            report_pending: true,
        })
    }
    /// Handle control messages and worker completions for all three roles.
    ///
    /// Trades retain their values, versions and admission permits in the pending
    /// queue; market notifications update the latest requested market. Metric
    /// changes invalidate pricing state and reset performance estimates.
    /// `PricingDone` applies only the current epoch's outcomes and records service
    /// time; `PublishDone` records delivery time and manages retry backoff.
    /// Messages that advance state then call `pump` to update scheduling and
    /// output; obsolete worker completions are ignored.
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        self.state_distr
            .incr_one(ProcessorMiddleMessageStates::from(&message));
        if !matches!(&message, ProcessorMessage::Wake) {
            state.report_pending = true;
        }
        match message {
            ProcessorMessage::NewTrade(delivery) => {
                let event = &delivery.event;
                if state
                    .known
                    .get(&event.id)
                    .is_none_or(|old| old.version < event.version)
                {
                    state.known.insert(event.id.clone(), event.clone());
                }
                if !state.completed(event) {
                    state.pending.push_back(delivery);
                }
            }
            ProcessorMessage::NewMarket => {
                state.requested_market = self.market_slot.lock().unwrap().take();
            }
            ProcessorMessage::NewTradePortfolio(delivery) => {
                self.accept_snapshot(state, delivery.update);
            }
            ProcessorMessage::Metric(mut metrics) => {
                metrics.sort_by_key(|m| m.to_string());
                metrics.dedup();
                if state.metrics != metrics {
                    state.worker = None;
                    state.epoch += 1;
                    state.metrics = metrics;
                    state.priced.clear();
                    state.local_ahead.clear();
                    state.failed.clear();
                    state.portfolio = PmPortfolio::new();
                    state.repair.clear();
                    state.refresh = self.role != Role::Current
                        || self.operating_mode != RTOperatingMode::SingleBuffer;
                    state.refreshing = false;
                    state.throttle.reset_workload(Instant::now());
                    state.dirty = true;
                }
            }
            ProcessorMessage::Wake => {
                state.timer = None;
            }
            ProcessorMessage::PricingDone(completion) => {
                if completion.epoch != state.epoch {
                    return Ok(());
                }
                state.worker = None;
                state.throttle.observe_pricing(
                    completion.elapsed,
                    completion.outcomes.len(),
                    Instant::now(),
                );
                for (event, result) in completion.outcomes {
                    match result {
                        Ok(results) => {
                            state.install(Arc::new(PricedTrade { event, results }));
                        }
                        Err(reason) => {
                            state.failures += 1;
                            state
                                .failed
                                .entry(event.id.clone())
                                .and_modify(|v| *v = (*v).max(event.version))
                                .or_insert(event.version);
                            warn!(processor=%self.processor_name, trade=%event.id, version=event.version, %reason, "Trade not priced; retained for retry on a new market or metric set");
                        }
                    }
                }
                state.refreshing = false;
                state.dirty = true;
            }
            ProcessorMessage::PublishDone {
                sequence,
                elapsed,
                result,
            } => {
                if sequence != state.publish_sequence {
                    return Ok(());
                }
                state.publishing = None;
                state.throttle.observe_publishing(elapsed, Instant::now());
                if let Err(reason) = result {
                    state.publish_failures = state.publish_failures.saturating_add(1);
                    let backoff =
                        Duration::from_millis(100 * (1u64 << state.publish_failures.min(5)));
                    state.publish_retry = Instant::now() + backoff;
                    state.dirty = true;
                    error!(processor=%self.processor_name, %reason, retry_ms=backoff.as_millis(), "Publishing failed; latest portfolio retained");
                } else {
                    state.publish_failures = 0;
                }
            }
        }
        self.pump(&myself, state);
        Ok(())
    }
    /// Close admission and snapshot capacity so blocked senders can observe
    /// shutdown. Worker and timer tasks are aborted when their state is dropped.
    async fn post_stop(
        &self,
        _: ActorRef<Self::Msg>,
        _: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // Wake blocked senders, including the Kafka ingress task.
        self.admission.close();
        self.snapshots.close();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine_actor::spawn_processor;
    use crate::markets::letf_market::{LETFMarketType, LETFMarketTypes};
    use crate::portfolio::{PV01Results, PortfolioType};
    use crate::portfolio_results::PortfolioResults;
    use crate::processor_curr::PortfolioPublisher;
    use crate::trade::{BaseTrade, TradeDirection};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{mpsc, Notify};

    #[derive(Clone, Debug)]
    struct TestTrade {
        id: String,
        value: f64,
        delay: Duration,
        calls: Arc<AtomicUsize>,
        blocked: Option<Arc<Notify>>,
    }
    impl BaseTrade for TestTrade {
        fn id(&self) -> String {
            self.id.clone()
        }
        fn direction(&self) -> TradeDirection {
            TradeDirection::Create
        }
    }
    #[async_trait]
    impl PriceTrade<LETFMarketType> for TestTrade {
        async fn initial_pv(&self) -> Option<f64> {
            Some(0.0)
        }
        async fn needs_recompute(&self, _: Arc<LETFMarketType>, _: Arc<LETFMarketType>) -> bool {
            true
        }
        async fn price(&self, market: Arc<LETFMarketType>) -> Option<f64> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(blocked) = &self.blocked {
                blocked.notified().await;
            }
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Some(self.value * market.get(&LETFMarketTypes::Stock("stock".into())).await?)
        }
        async fn pv01(&self, _: Arc<LETFMarketType>) -> PV01Results {
            PV01Results::new()
        }
    }

    struct Publisher {
        output: mpsc::UnboundedSender<PmPortfolio>,
        calls: Arc<AtomicUsize>,
        blocked: Option<Arc<Notify>>,
    }
    #[async_trait]
    impl PortfolioPublisher for Publisher {
        async fn publish(&self, portfolio: PmPortfolio) -> Result<(), String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(blocked) = &self.blocked {
                blocked.notified().await;
            }
            self.output.send(portfolio).map_err(|e| e.to_string())
        }
    }
    fn publisher(
        blocked: Option<Arc<Notify>>,
    ) -> (
        SharedPublisher,
        mpsc::UnboundedReceiver<PmPortfolio>,
        Arc<AtomicUsize>,
    ) {
        let (output, receiver) = mpsc::unbounded_channel();
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Publisher {
                output,
                calls: calls.clone(),
                blocked,
            }),
            receiver,
            calls,
        )
    }
    fn config() -> ThrottleConfig {
        ThrottleConfig {
            capacity: 8,
            burst: 2,
            stall_timeout: Duration::from_millis(100),
            smoothing: Duration::from_millis(100),
            worker_timeout: Duration::from_secs(2),
            ..ThrottleConfig::default()
        }
    }
    fn event(id: &str, version: u64, value: f64) -> Arc<TradeEvent<TestTrade>> {
        Arc::new(TradeEvent {
            id: id.into(),
            version,
            received: Instant::now(),
            value: TestTrade {
                id: id.into(),
                value,
                delay: Duration::ZERO,
                calls: Arc::new(AtomicUsize::new(0)),
                blocked: None,
            },
        })
    }
    async fn market(name: &str, stock: Option<f64>) -> Arc<LETFMarketType> {
        let market = Arc::new(LETFMarketType::new(name.into()));
        if let Some(stock) = stock {
            market
                .insert(LETFMarketTypes::Stock("stock".into()), stock)
                .await;
        }
        market
    }
    async fn spawn(
        name: &str,
        role: Role,
        below: Option<Endpoint<TestTrade, LETFMarketType>>,
        publisher: Option<SharedPublisher>,
        results: Arc<PortfolioResults>,
        config: ThrottleConfig,
    ) -> (Endpoint<TestTrade, LETFMarketType>, JoinHandle<()>) {
        let (endpoint, task, _) = spawn_processor(
            name.into(),
            role,
            below,
            publisher,
            RTOperatingMode::DoubleBuffer,
            Arc::new(AllMarkets::new()),
            results.reporter("test".into(), name, name.into()),
            PricingStyle::Parallel,
            config,
        )
        .await;
        endpoint
            .actor
            .send_message(ProcessorMessage::Metric(vec![PricingMetric::PV]))
            .unwrap();
        (endpoint, task)
    }
    async fn snapshot(
        endpoint: &Endpoint<TestTrade, LETFMarketType>,
        sequence: u64,
        market: Arc<LETFMarketType>,
        events: Vec<(Arc<TradeEvent<TestTrade>>, f64)>,
    ) {
        let trades = events
            .into_iter()
            .map(|(event, value)| {
                let mut results = PmPortfolio::new();
                results.insert(
                    PricingMetric::PV,
                    PortfolioType::from([(event.id.clone(), value)]),
                );
                (event.id.clone(), Arc::new(PricedTrade { event, results }))
            })
            .collect();
        let permit = endpoint.snapshots.clone().acquire_owned().await.unwrap();
        endpoint
            .actor
            .send_message(ProcessorMessage::NewTradePortfolio(PortfolioDelivery {
                update: PortfolioUpdate {
                    sequence,
                    market,
                    metrics: vec![PricingMetric::PV],
                    trades,
                    failed: HashMap::new(),
                },
                _permit: permit,
            }))
            .unwrap();
    }
    async fn wait(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }
    async fn wait_portfolio(
        receiver: &mut mpsc::UnboundedReceiver<PmPortfolio>,
        predicate: impl Fn(&PortfolioType) -> bool,
    ) -> PortfolioType {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let portfolio = receiver.recv().await.expect("publisher closed");
                if let Some(pv) = portfolio.get(&PricingMetric::PV) {
                    if predicate(pv) {
                        return pv.clone();
                    }
                }
            }
        })
        .await
        .expect("expected portfolio was not published")
    }
    async fn stop(endpoint: Endpoint<TestTrade, LETFMarketType>, task: JoinHandle<()>) {
        endpoint.actor.stop(None);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn burst_pipeline_preserves_updates_and_replaces_values_without_double_counting() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, mut output, _) = publisher(None);
        let (current, current_task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results.clone(),
            config(),
        )
        .await;
        let (middle, middle_task) = spawn(
            "middle",
            Role::Middle,
            Some(current.clone()),
            None,
            results.clone(),
            config(),
        )
        .await;
        let (new, new_task) = spawn(
            "new",
            Role::New,
            Some(middle.clone()),
            None,
            results.clone(),
            config(),
        )
        .await;
        new.market(market("market", Some(2.0)).await).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            for i in 0..40 {
                let event = event(&format!("t{i}"), i + 1, i as f64);
                for target in [&new, &middle, &current] {
                    target.send_trade(event.clone()).await.unwrap();
                }
            }
            let update = event("t0", 41, 100.0);
            for target in [&new, &middle, &current] {
                target.send_trade(update.clone()).await.unwrap();
            }
        })
        .await
        .expect("bounded fanout deadlocked");
        let portfolio = wait_portfolio(&mut output, |pv| {
            pv.len() == 40 && pv.get("t0") == Some(&200.0)
        })
        .await;
        assert_eq!(portfolio.values().sum::<f64>(), 1760.0);
        wait(|| {
            current.admission.available_permits() == 8
                && middle.admission.available_permits() == 8
                && new.admission.available_permits() == 8
        })
        .await;
        for result in results.snapshot().get("test").unwrap().values() {
            assert!(result.throttle.as_ref().unwrap().pending <= 8);
        }
        stop(new, new_task).await;
        stop(middle, middle_task).await;
        stop(current, current_task).await;
    }

    #[tokio::test]
    async fn newer_snapshot_interrupts_slow_pricing_and_obsolete_completion_cannot_overwrite_it() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, mut output, _) = publisher(None);
        let (current, task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results.clone(),
            config(),
        )
        .await;
        snapshot(&current, 1, market("old", Some(1.0)).await, vec![]).await;
        let blocked = Arc::new(Notify::new());
        let mut slow = event("trade", 1, 10.0);
        Arc::get_mut(&mut slow).unwrap().value.blocked = Some(blocked.clone());
        let calls = slow.value.calls.clone();
        current.send_trade(slow).await.unwrap();
        wait(|| calls.load(Ordering::SeqCst) == 1).await;
        snapshot(
            &current,
            2,
            market("new", Some(10.0)).await,
            vec![(event("trade", 2, 20.0), 200.0)],
        )
        .await;
        wait_portfolio(&mut output, |pv| pv.get("trade") == Some(&200.0)).await;
        assert_eq!(current.admission.available_permits(), 8);
        blocked.notify_waiters();
        wait(|| {
            results
                .snapshot()
                .get("test")
                .and_then(|p| p.get("current"))
                .is_some_and(|r| r.market.as_deref() == Some("new"))
        })
        .await;
        assert_eq!(
            results.snapshot()["test"]["current"].metrics["PV"].total,
            Some(200.0)
        );
        stop(current, task).await;
    }

    #[tokio::test]
    async fn slow_kafka_does_not_block_snapshot_handling_or_create_parallel_publishers() {
        let results = Arc::new(PortfolioResults::default());
        let blocked = Arc::new(Notify::new());
        let (publisher, mut output, calls) = publisher(Some(blocked.clone()));
        let (current, task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results.clone(),
            config(),
        )
        .await;
        snapshot(&current, 1, market("old", Some(1.0)).await, vec![]).await;
        wait(|| calls.load(Ordering::SeqCst) == 1).await;
        snapshot(
            &current,
            2,
            market("new", Some(3.0)).await,
            vec![(event("trade", 1, 4.0), 12.0)],
        )
        .await;
        wait(|| {
            results
                .snapshot()
                .get("test")
                .and_then(|p| p.get("current"))
                .is_some_and(|r| r.market.as_deref() == Some("new"))
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        blocked.notify_one();
        wait(|| calls.load(Ordering::SeqCst) == 2).await;
        blocked.notify_one();
        wait_portfolio(&mut output, |pv| pv.get("trade") == Some(&12.0)).await;
        stop(current, task).await;
    }

    #[tokio::test]
    async fn worker_timeout_releases_admission_and_does_not_advertise_successful_coverage() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, mut output, _) = publisher(None);
        let mut config = config();
        config.worker_timeout = Duration::from_millis(20);
        let (current, task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results.clone(),
            config,
        )
        .await;
        snapshot(&current, 1, market("market", Some(1.0)).await, vec![]).await;
        let mut slow = event("slow", 1, 10.0);
        Arc::get_mut(&mut slow).unwrap().value.blocked = Some(Arc::new(Notify::new()));
        let calls = slow.value.calls.clone();
        current.send_trade(slow).await.unwrap();
        wait(|| calls.load(Ordering::SeqCst) > 0 && current.admission.available_permits() == 8)
            .await;
        current.send_trade(event("good", 2, 3.0)).await.unwrap();
        let portfolio = wait_portfolio(&mut output, |pv| pv.get("good") == Some(&3.0)).await;
        assert!(!portfolio.contains_key("slow"));
        wait(|| {
            results.snapshot()["test"]["current"]
                .throttle
                .as_ref()
                .unwrap()
                .failed_trades
                == 1
        })
        .await;
        stop(current, task).await;
    }

    #[tokio::test]
    async fn unpriceable_versions_release_credits_and_retry_on_a_new_market() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, mut output, _) = publisher(None);
        let (current, current_task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results.clone(),
            config(),
        )
        .await;
        let (new, new_task) = spawn(
            "new",
            Role::New,
            Some(current.clone()),
            None,
            results.clone(),
            config(),
        )
        .await;
        new.market(market("missing", None).await).unwrap();
        let trade = event("trade", 1, 5.0);
        new.send_trade(trade.clone()).await.unwrap();
        current.send_trade(trade).await.unwrap();
        wait(|| {
            results
                .snapshot()
                .get("test")
                .and_then(|p| p.get("new"))
                .and_then(|r| r.throttle.as_ref())
                .is_some_and(|t| t.failed_trades > 0)
        })
        .await;
        wait(|| {
            new.admission.available_permits() == 8 && current.admission.available_permits() == 8
        })
        .await;
        new.market(market("available", Some(2.0)).await).unwrap();
        wait_portfolio(&mut output, |pv| pv.get("trade") == Some(&10.0)).await;
        stop(new, new_task).await;
        stop(current, current_task).await;
    }

    #[tokio::test]
    async fn admission_is_held_after_receiving_and_waiters_wake_when_actor_stops() {
        let results = Arc::new(PortfolioResults::default());
        let mut config = config();
        config.capacity = 2;
        let (current, task) = spawn("current", Role::Current, None, None, results, config).await;
        // No market: the actor can receive messages, but cannot finish pricing.
        current.send_trade(event("a", 1, 1.0)).await.unwrap();
        current.send_trade(event("b", 2, 1.0)).await.unwrap();
        assert_eq!(current.admission.available_permits(), 0);
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            current.send_trade(event("c", 3, 1.0))
        )
        .await
        .is_err());
        let waiting = {
            let current = current.clone();
            tokio::spawn(async move { current.send_trade(event("c", 3, 1.0)).await })
        };
        stop(current, task).await;
        assert!(tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .is_err());
    }

    #[tokio::test]
    async fn single_buffer_uses_upstream_coverage_without_repricing_trades() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, mut output, _) = publisher(None);
        let (current, current_task, _) = spawn_processor(
            "current".into(),
            Role::Current,
            None,
            Some(publisher),
            RTOperatingMode::SingleBuffer,
            Arc::new(AllMarkets::new()),
            results.reporter("test".into(), "current", "current".into()),
            PricingStyle::Sequential,
            config(),
        )
        .await;
        current
            .actor
            .send_message(ProcessorMessage::Metric(vec![PricingMetric::PV]))
            .unwrap();
        let (new, new_task) = spawn(
            "new",
            Role::New,
            Some(current.clone()),
            None,
            results,
            config(),
        )
        .await;
        new.market(market("market", Some(2.0)).await).unwrap();
        let trade = event("trade", 1, 3.0);
        let calls = trade.value.calls.clone();
        new.send_trade(trade.clone()).await.unwrap();
        current.send_trade(trade).await.unwrap();
        wait_portfolio(&mut output, |pv| pv.get("trade") == Some(&6.0)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        wait(|| current.admission.available_permits() == 8).await;
        stop(new, new_task).await;
        stop(current, current_task).await;
    }

    struct HoldingSink(Arc<Notify>, Arc<AtomicUsize>);
    #[async_trait]
    impl Actor for HoldingSink {
        type Msg = ProcessorMessage<TestTrade, LETFMarketType>;
        type State = ();
        type Arguments = ();
        async fn pre_start(&self, _: ActorRef<Self::Msg>, _: ()) -> Result<(), ActorProcessingErr> {
            Ok(())
        }
        async fn handle(
            &self,
            _: ActorRef<Self::Msg>,
            message: Self::Msg,
            _: &mut (),
        ) -> Result<(), ActorProcessingErr> {
            if let ProcessorMessage::NewTradePortfolio(delivery) = message {
                self.1.fetch_add(1, Ordering::SeqCst);
                self.0.notified().await;
                drop(delivery);
            }
            Ok(())
        }
    }
    #[tokio::test]
    async fn slow_snapshot_receiver_bounds_both_snapshot_and_trade_backlogs() {
        let release = Arc::new(Notify::new());
        let count = Arc::new(AtomicUsize::new(0));
        let (actor, sink_task) =
            Actor::spawn(None, HoldingSink(release.clone(), count.clone()), ())
                .await
                .unwrap();
        let sink = Endpoint {
            actor,
            admission: Arc::new(Semaphore::new(2)),
            snapshots: Arc::new(Semaphore::new(1)),
            market_slot: Arc::new(Mutex::new(None)),
        };
        let results = Arc::new(PortfolioResults::default());
        let mut config = config();
        config.capacity = 2;
        let (new, task) = spawn("new", Role::New, Some(sink.clone()), None, results, config).await;
        new.market(market("market", Some(1.0)).await).unwrap();
        wait(|| count.load(Ordering::SeqCst) == 1).await;
        // One batch can finish locally while the initial snapshot is held. The
        // next batch is admitted but cannot start until the output slot frees.
        new.send_trade(event("a", 1, 1.0)).await.unwrap();
        wait(|| new.admission.available_permits() == 2).await;
        for i in 0..2 {
            new.send_trade(event(&format!("b{i}"), i + 3, 1.0))
                .await
                .unwrap();
        }
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            new.send_trade(event("overflow", 5, 1.0))
        )
        .await
        .is_err());
        assert_eq!(sink.snapshots.available_permits(), 0);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        release.notify_one();
        wait(|| count.load(Ordering::SeqCst) == 2).await;
        stop(new, task).await;
        sink.actor.stop(None);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), sink_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn rejected_snapshots_still_update_lead_and_do_not_prevent_later_acceptance() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, mut output, _) = publisher(None);
        let (current, task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results.clone(),
            config(),
        )
        .await;
        let trades: Vec<_> = (0..10)
            .map(|i| (event(&format!("t{i}"), i + 1, 1.0), 1.0))
            .collect();
        snapshot(&current, 1, market("old", Some(1.0)).await, trades.clone()).await;
        wait_portfolio(&mut output, |pv| pv.len() == 10).await;
        snapshot(
            &current,
            2,
            market("rejected", Some(2.0)).await,
            vec![(event("other", 11, 1.0), 2.0)],
        )
        .await;
        wait(|| {
            results
                .snapshot()
                .get("test")
                .and_then(|p| p.get("current"))
                .and_then(|r| r.throttle.as_ref())
                .is_some_and(|t| t.lead == 10)
        })
        .await;
        assert_eq!(
            results.snapshot()["test"]["current"].market.as_deref(),
            Some("old")
        );
        snapshot(
            &current,
            3,
            market("accepted", Some(2.0)).await,
            trades.into_iter().map(|(e, _)| (e, 2.0)).collect(),
        )
        .await;
        wait_portfolio(&mut output, |pv| pv.values().sum::<f64>() == 20.0).await;
        stop(current, task).await;
    }

    #[tokio::test]
    async fn accepted_portfolio_repairs_do_not_wait_for_the_lead_stall_timeout() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, mut output, _) = publisher(None);
        let mut config = config();
        config.stall_timeout = Duration::from_secs(5);
        let (current, task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results,
            config,
        )
        .await;
        let trades = (0..9)
            .map(|i| (event(&format!("t{i}"), i + 1, 1.0), 1.0))
            .collect();
        snapshot(&current, 1, market("old", Some(1.0)).await, trades).await;
        wait_portfolio(&mut output, |pv| pv.len() == 9).await;
        snapshot(&current, 2, market("new", Some(2.0)).await, vec![]).await;
        tokio::time::timeout(
            Duration::from_millis(500),
            wait_portfolio(&mut output, |pv| pv.values().sum::<f64>() == 18.0),
        )
        .await
        .expect("accepted portfolio repairs were incorrectly lead-throttled");
        stop(current, task).await;
    }

    struct FlakyPublisher(mpsc::UnboundedSender<PmPortfolio>, Arc<AtomicUsize>);
    #[async_trait]
    impl PortfolioPublisher for FlakyPublisher {
        async fn publish(&self, portfolio: PmPortfolio) -> Result<(), String> {
            if self.1.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err("temporary Kafka failure".into());
            }
            self.0.send(portfolio).map_err(|e| e.to_string())
        }
    }
    #[tokio::test]
    async fn publishing_retry_uses_latest_state_and_keeps_control_messages_responsive() {
        let results = Arc::new(PortfolioResults::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (output, mut receiver) = mpsc::unbounded_channel();
        let publisher = Arc::new(FlakyPublisher(output, calls.clone()));
        let (current, task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results,
            config(),
        )
        .await;
        snapshot(&current, 1, market("old", Some(1.0)).await, vec![]).await;
        wait(|| calls.load(Ordering::SeqCst) == 1).await;
        snapshot(
            &current,
            2,
            market("new", Some(1.0)).await,
            vec![(event("trade", 1, 42.0), 42.0)],
        )
        .await;
        let latest = wait_portfolio(&mut receiver, |pv| pv.get("trade") == Some(&42.0)).await;
        assert_eq!(latest.len(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        stop(current, task).await;
    }

    #[tokio::test]
    async fn changing_metrics_cancels_old_pricing_and_rebuilds_successful_coverage() {
        let results = Arc::new(PortfolioResults::default());
        let (publisher, _output, _) = publisher(None);
        let (current, task) = spawn(
            "current",
            Role::Current,
            None,
            Some(publisher),
            results.clone(),
            config(),
        )
        .await;
        snapshot(&current, 1, market("market", Some(1.0)).await, vec![]).await;
        let mut slow = event("trade", 1, 2.0);
        Arc::get_mut(&mut slow).unwrap().value.blocked = Some(Arc::new(Notify::new()));
        let calls = slow.value.calls.clone();
        current.send_trade(slow).await.unwrap();
        wait(|| calls.load(Ordering::SeqCst) == 1).await;
        current
            .actor
            .send_message(ProcessorMessage::Metric(vec![PricingMetric::PV01]))
            .unwrap();
        wait(|| {
            results
                .snapshot()
                .get("test")
                .and_then(|p| p.get("current"))
                .is_some_and(|p| p.metrics.contains_key("PV01"))
        })
        .await;
        let snapshot = results.snapshot();
        assert!(!snapshot["test"]["current"].metrics.contains_key("PV"));
        assert_eq!(current.admission.available_permits(), 8);
        stop(current, task).await;
    }
}
