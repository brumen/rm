//! Adaptive admission policy. All times are monotonic; intentional waiting is
//! never included in measured service time.
//!
//! A token represents permission to start pricing one trade. Tokens accumulate
//! at an adjustable rate, up to the configured burst size. New uses measured
//! local pricing capacity; Middle and Current also follow upstream progress so
//! they do not get too far ahead for an incoming portfolio to be accepted.
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct ThrottleConfig {
    pub capacity: usize,
    pub burst: usize,
    pub target_lead: usize,
    pub smoothing: Duration,
    pub recovery: Duration,
    pub stall_timeout: Duration,
    pub worker_timeout: Duration,
    pub initial_rate: f64,
}

impl Default for ThrottleConfig {
    /// Supply the starting limits and timing settings used when no environment
    /// overrides are present. Rates are trades per second; lead counts local
    /// trade versions that the latest upstream portfolio has not covered.
    fn default() -> Self {
        Self {
            capacity: 256,
            burst: 4,
            target_lead: 4,
            smoothing: Duration::from_secs(1),
            recovery: Duration::from_millis(500),
            stall_timeout: Duration::from_secs(2),
            worker_timeout: Duration::from_secs(30),
            initial_rate: 2_000.0,
        }
    }
}

impl ThrottleConfig {
    /// Read optional RT_THROTTLE_* overrides, keeping defaults for unset values.
    /// Reject invalid or out-of-range values so startup fails with the setting's
    /// name rather than running with an unusable pacing configuration.
    pub fn from_env() -> Result<Self, String> {
        let mut c = Self::default();

        /// Read a finite positive number, or use the default when it is unset.
        /// this number is read from the environment.
        fn number(name: &str, default: f64) -> Result<f64, String> {
            match std::env::var(name) {
                Err(std::env::VarError::NotPresent) => Ok(default),
                Ok(v) => v
                    .parse::<f64>()
                    .ok()
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .ok_or_else(|| format!("{name} must be a finite positive number")),
                Err(e) => Err(format!("{name}: {e}")),
            }
        }
        /// Apply the same parsing rules, then require a whole number in 1..=max.
        fn integer(name: &str, default: usize, max: usize) -> Result<usize, String> {
            let n = number(name, default as f64)?;
            if n.fract() != 0.0 || n > max as f64 {
                return Err(format!("{name} must be an integer in 1..={max}"));
            }
            Ok(n as usize)
        }
        c.capacity = integer("RT_THROTTLE_CAPACITY", c.capacity, 65_536)?;
        c.burst = integer("RT_THROTTLE_BURST", c.burst, 8)?.min(c.capacity);
        c.target_lead = integer("RT_THROTTLE_TARGET_LEAD", c.target_lead, 7)?;
        for (name, value) in [
            ("RT_THROTTLE_EWMA_MS", &mut c.smoothing),
            ("RT_THROTTLE_RECOVERY_MS", &mut c.recovery),
            ("RT_THROTTLE_STALL_MS", &mut c.stall_timeout),
            ("RT_THROTTLE_WORKER_TIMEOUT_MS", &mut c.worker_timeout),
        ] {
            let ms = integer(name, value.as_millis() as usize, 3_600_000)?;
            *value = Duration::from_millis(ms as u64);
        }
        c.initial_rate = number("RT_THROTTLE_INITIAL_RATE", c.initial_rate)?;
        if c.initial_rate > 1_000_000.0 {
            return Err("RT_THROTTLE_INITIAL_RATE exceeds 1000000".into());
        }
        Ok(c)
    }
}

#[derive(Clone, Debug, Default)]
struct Ewma {
    value: Option<f64>,
    sampled: Option<Instant>,
}
impl Ewma {
    /// Smooth a measurement with an exponentially weighted moving average.
    /// The first valid sample initializes the average. Later samples receive
    /// more weight when more time has passed since the previous sample; the
    /// smoothing duration controls how quickly old measurements lose influence.
    /// Ignore negative or non-finite samples so they cannot corrupt the rate.
    fn observe(&mut self, sample: f64, now: Instant, smoothing: Duration) {
        if !sample.is_finite() || sample < 0.0 {
            return;
        }
        self.value = Some(match (self.value, self.sampled) {
            (Some(old), Some(last)) => {
                let elapsed = now.saturating_duration_since(last).as_secs_f64();
                let alpha = -(-elapsed / smoothing.as_secs_f64()).exp_m1();
                old + alpha * (sample - old)
            }
            _ => sample,
        });
        self.sampled = Some(now);
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ThrottleSnapshot {
    pub pending: usize,
    pub oldest_pending_ms: f64,
    pub lead: usize,
    pub target_lead: usize,
    pub rate_per_second: f64,
    pub pricing_ms_per_trade: Option<f64>,
    pub publishing_ms: Option<f64>,
    pub upstream_rate_per_second: Option<f64>,
    pub upstream_stalled: bool,
    pub failed_trades: u64,
}

#[derive(Debug)]
pub(crate) struct AdaptiveThrottle {
    /// Limits, timing settings and initial rate shared with the processor scheduler.
    pub config: ThrottleConfig,
    /// Smoothed pricing time in seconds per trade; used to estimate local capacity.
    pricing: Ewma,
    /// Smoothed duration of a publication attempt, in seconds; for diagnostics only.
    publishing: Ewma,
    /// Smoothed upstream progress in trades per second, measured while work is pending.
    upstream_rate: Ewma,
    /// Highest upstream version seen for each trade ID since initialization or reset.
    /// Prevents repeated coverage from being mistaken for new progress.
    highwater: HashMap<String, u64>,
    /// Trade ID to successfully priced version in the latest observed upstream portfolio.
    /// Used to check coverage and lead; may omit versions previously seen in highwater.
    upstream: HashMap<String, u64>,
    /// Time upstream last advanced beyond highwater; initialized to the start/reset time.
    /// Used to measure progress rate and detect stalls.
    progress_at: Instant,
    /// Start of the current period with pending trades or repairs; None while idle.
    /// Excludes earlier idle time from progress-rate measurements and stall detection.
    busy_since: Option<Instant>,
    /// Available pricing credits, capped at config.burst. One whole token lets one
    /// trade start pricing; fractional credits accumulate until a full token is earned.
    tokens: f64,
    /// Time tokens were last replenished; elapsed time determines credits earned.
    refilled: Instant,
    /// Current token refill rate in trades per second, adjusted to local pricing
    /// capacity and, when upstream pacing is enabled, upstream progress and local lead.
    rate: f64,
}

impl AdaptiveThrottle {
    /// Start with a full burst of tokens and the configured initial rate.
    /// Service-time and upstream-rate estimates remain unknown until observed;
    /// stall detection starts only when the caller reports pending work.
    pub fn new(config: ThrottleConfig, now: Instant) -> Self {
        Self {
            tokens: config.burst as f64,
            rate: config.initial_rate,
            config,
            pricing: Ewma::default(),
            publishing: Ewma::default(),
            upstream_rate: Ewma::default(),
            highwater: HashMap::new(),
            upstream: HashMap::new(),
            progress_at: now,
            busy_since: None,
            refilled: now,
        }
    }
    /// Keep the configuration but clear measurements, coverage and stall history.
    /// Restore the initial rate and full burst, for example when a metric change
    /// makes the previous workload's performance estimates irrelevant.
    pub fn reset_workload(&mut self, now: Instant) {
        let config = self.config.clone();
        *self = Self::new(config, now);
    }
    /// Record a completed pricing job's elapsed time divided by its trade count.
    /// The reciprocal of this smoothed time estimates local trades per second.
    /// The caller supplies worker time only, excluding queueing and pacing waits;
    /// an empty job contributes no sample.
    pub fn observe_pricing(&mut self, elapsed: Duration, count: usize, now: Instant) {
        if count > 0 {
            self.pricing.observe(
                elapsed.as_secs_f64() / count as f64,
                now,
                self.config.smoothing,
            );
        }
    }
    /// Record a publication attempt's duration for diagnostics. This measurement
    /// does not directly change the pricing rate; output backpressure is handled
    /// by the processor scheduler.
    pub fn observe_publishing(&mut self, elapsed: Duration, now: Instant) {
        self.publishing
            .observe(elapsed.as_secs_f64(), now, self.config.smoothing);
    }
    /// Remember the successfully priced trade versions in an upstream portfolio.
    /// Each trade that advances beyond its highest previously seen version counts
    /// as one unit of progress, even if its version number jumps several steps.
    /// While busy, use that progress to estimate upstream trades per second.
    ///
    /// Duplicate or older coverage does not reset the stall clock. Keep the latest
    /// portfolio's coverage separately from the highest versions ever seen: lead
    /// must reflect what this portfolio covers, including missing contributions.
    pub fn observe_upstream(&mut self, coverage: HashMap<String, u64>, now: Instant) {
        let mut advanced = 0;
        for (id, version) in &coverage {
            let known = self.highwater.entry(id.clone()).or_default();
            if *version > *known {
                *known = *version;
                advanced += 1;
            }
        }
        if advanced > 0 {
            // Do not infer capacity from time spent idle before work arrived.
            if let Some(busy) = self.busy_since {
                let elapsed = now
                    .saturating_duration_since(busy.max(self.progress_at))
                    .as_secs_f64();
                if elapsed > 0.0 {
                    self.upstream_rate.observe(
                        advanced as f64 / elapsed,
                        now,
                        self.config.smoothing,
                    );
                }
            }
            self.progress_at = now;
        }
        self.upstream = coverage;
    }
    /// Check whether the latest upstream portfolio contains this trade version
    /// or a newer one. Trade events use positive versions; an absent ID counts as
    /// version zero and therefore does not cover a real event.
    pub fn covered_upstream(&self, id: &str, version: u64) -> bool {
        self.upstream.get(id).copied().unwrap_or(0) >= version
    }
    #[cfg(test)]
    /// Count local trade versions missing from, or newer than, upstream coverage.
    /// Updating an existing trade can increase lead without adding a new trade ID.
    pub fn lead<'a>(&self, versions: impl Iterator<Item = (&'a String, u64)>) -> usize {
        versions
            .filter(|(id, v)| self.upstream.get(*id).copied().unwrap_or(0) < *v)
            .count()
    }
    /// Report whether pending work has waited through the upstream stall timeout
    /// without coverage progress. Count from the later of becoming busy or the
    /// last progress, so idle time cannot cause an immediate stall.
    /// This measures trade coverage progress, not the age of the current market.
    pub fn stalled(&self, now: Instant) -> bool {
        self.busy_since.is_some_and(|busy| {
            now.saturating_duration_since(busy.max(self.progress_at)) >= self.config.stall_timeout
        })
    }
    /// Tell the controller whether incremental trades or repairs are pending.
    /// Start the busy clock on the idle-to-busy transition; repeated busy reports
    /// keep that start time. Clear it when the queue becomes idle.
    pub fn set_busy(&mut self, busy: bool, now: Instant) {
        if !busy {
            self.busy_since = None;
        } else if self.busy_since.is_none() {
            self.busy_since = Some(now);
        }
    }
    /// Accumulate tokens earned since the last call, then update the earning rate.
    /// `lead` counts local versions not covered upstream; `paced` enables following
    /// upstream rather than using local pricing capacity alone.
    ///
    /// Normal pacing follows the measured upstream rate, adjusted to close the
    /// gap between actual and target lead over the recovery duration. Ignore a
    /// one-trade error to avoid constant small corrections. Cap the target by
    /// local capacity and smooth rate changes over 100 ms.
    ///
    /// At a lead of eight, withhold tokens to preserve acceptance headroom. If
    /// upstream stalls, relax that restriction and use a conservative rate capped
    /// at 10% of the initial rate, allowing local work to continue.
    fn refill(&mut self, lead: usize, paced: bool, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refilled).as_secs_f64();
        // Refill using the previous rate so lowering the rate cannot erase time
        // already earned, and raising it cannot create a retrospective burst.
        self.tokens = (self.tokens + elapsed * self.rate).min(self.config.burst as f64);
        self.refilled = now;
        let capacity = self
            .pricing
            .value
            .map(|s| 1.0 / s.max(1e-6))
            .unwrap_or(self.config.initial_rate)
            .min(1_000_000.0);
        let target = if !paced {
            capacity
        } else if self.stalled(now) {
            capacity.min(self.config.initial_rate * 0.1)
        } else {
            let error = self.config.target_lead as f64 - lead as f64;
            let adjustment = if error.abs() <= 1.0 {
                0.0
            } else {
                error / self.config.recovery.as_secs_f64()
            };
            (self.upstream_rate.value.unwrap_or(capacity) + adjustment).clamp(0.0, capacity)
        };
        let alpha = -(-elapsed / 0.1).exp_m1();
        self.rate = (self.rate + alpha * (target - self.rate)).clamp(0.0, capacity);
        if paced && self.stalled(now) {
            self.rate = self.rate.min(target);
        }
        // Preserve headroom below the sink's ten-trade rejection threshold.
        if paced && lead >= 8 && !self.stalled(now) {
            self.tokens = 0.0;
        }
    }
    /// Return how many of the requested trades may start now, spending one token
    /// per trade. Limit the count by available whole tokens and, during normal
    /// upstream pacing, the remaining room below a lead of eight.
    /// Returns zero when work must wait; this method never sleeps or queues it.
    pub fn take(&mut self, requested: usize, lead: usize, paced: bool, now: Instant) -> usize {
        self.refill(lead, paced, now);
        let headroom = if paced && !self.stalled(now) {
            8usize.saturating_sub(lead)
        } else {
            requested
        };
        let count = requested.min(self.tokens.floor() as usize).min(headroom);
        self.tokens -= count as f64;
        count
    }
    /// Suggest when the actor should check for work again, without waiting here.
    /// Estimate the time to earn the next token, bounded to 1-50 ms so control
    /// stays responsive even when the calculated pricing rate is near zero.
    pub fn wake_delay(&self) -> Duration {
        if self.tokens >= 1.0 {
            return Duration::from_millis(1);
        }
        Duration::from_secs_f64(((1.0 - self.tokens) / self.rate.max(1.0)).clamp(0.001, 0.05))
    }
    /// Combine controller measurements with queue and failure counts supplied by
    /// the actor for display in diagnostics. Convert internal seconds to
    /// milliseconds; measurements remain None until their first valid sample.
    /// Capturing a snapshot does not spend tokens or change controller state.
    pub fn snapshot(
        &self,
        pending: usize,
        oldest: Option<Instant>,
        lead: usize,
        failures: u64,
        now: Instant,
    ) -> ThrottleSnapshot {
        ThrottleSnapshot {
            pending,
            oldest_pending_ms: oldest.map_or(0.0, |at| {
                now.saturating_duration_since(at).as_secs_f64() * 1000.0
            }),
            lead,
            target_lead: self.config.target_lead,
            rate_per_second: self.rate,
            pricing_ms_per_trade: self.pricing.value.map(|v| v * 1000.0),
            publishing_ms: self.publishing.value.map(|v| v * 1000.0),
            upstream_rate_per_second: self.upstream_rate.value,
            upstream_stalled: self.stalled(now),
            failed_trades: failures,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn versions_measure_updates_not_just_portfolio_size() {
        let now = Instant::now();
        let mut t = AdaptiveThrottle::new(ThrottleConfig::default(), now);
        t.observe_upstream(HashMap::from([("a".into(), 1)]), now);
        let local = HashMap::from([("a".into(), 2), ("b".into(), 1)]);
        assert_eq!(t.lead(local.iter().map(|(id, v)| (id, *v))), 2);
        t.observe_upstream(local.clone(), now + Duration::from_millis(10));
        assert_eq!(t.lead(local.iter().map(|(id, v)| (id, *v))), 0);
    }
    #[test]
    fn burst_is_bounded_and_stalled_upstream_eventually_releases_work() {
        let now = Instant::now();
        let mut t = AdaptiveThrottle::new(ThrottleConfig::default(), now);
        t.set_busy(true, now);
        assert_eq!(t.take(100, 0, true, now), 4);
        assert_eq!(t.take(100, 0, true, now), 0);
        assert_eq!(t.take(100, 8, true, now + Duration::from_secs(1)), 0);
        assert!(t.take(100, 8, true, now + Duration::from_secs(3)) > 0);
    }
    #[test]
    fn ewma_uses_fractional_seconds_and_ignores_intentional_waiting() {
        let now = Instant::now();
        let mut t = AdaptiveThrottle::new(ThrottleConfig::default(), now);
        t.observe_pricing(Duration::from_micros(500), 1, now);
        t.observe_pricing(
            Duration::from_millis(2),
            1,
            now + Duration::from_millis(100),
        );
        let s = t.snapshot(0, None, 0, 0, now);
        assert!(s.pricing_ms_per_trade.unwrap() > 0.5 && s.pricing_ms_per_trade.unwrap() < 2.0);
        t.take(1, 0, true, now + Duration::from_secs(30));
        assert_eq!(
            t.snapshot(0, None, 0, 0, now).pricing_ms_per_trade,
            s.pricing_ms_per_trade
        );
    }
    #[test]
    fn duplicate_snapshots_do_not_mask_stalls_and_idle_time_does_not_trigger_one() {
        let now = Instant::now();
        let mut t = AdaptiveThrottle::new(ThrottleConfig::default(), now);
        let coverage = HashMap::from([("a".into(), 1)]);
        t.observe_upstream(coverage.clone(), now);
        t.set_busy(true, now);
        t.observe_upstream(coverage, now + Duration::from_secs(3));
        assert!(t.stalled(now + Duration::from_secs(3)));
        t.set_busy(false, now + Duration::from_secs(3));
        t.set_busy(true, now + Duration::from_secs(30));
        assert!(!t.stalled(now + Duration::from_secs(30)));
        let mut idle = AdaptiveThrottle::new(ThrottleConfig::default(), now);
        idle.observe_upstream(
            HashMap::from([("a".into(), 1)]),
            now + Duration::from_secs(60),
        );
        assert!(idle
            .snapshot(0, None, 0, 0, now)
            .upstream_rate_per_second
            .is_none());
    }
    #[test]
    fn pacing_follows_measured_upstream_rate_and_capacity_recovers() {
        let now = Instant::now();
        let mut t = AdaptiveThrottle::new(ThrottleConfig::default(), now);
        t.set_busy(true, now);
        t.observe_pricing(Duration::from_millis(2), 1, now);
        t.observe_upstream(
            HashMap::from([("a".into(), 1)]),
            now + Duration::from_millis(100),
        );
        t.take(0, 6, true, now + Duration::from_millis(100));
        t.take(0, 6, true, now + Duration::from_millis(200));
        let slow = t.snapshot(1, Some(now), 6, 0, now);
        assert!(slow.rate_per_second < 500.0);
        t.observe_pricing(Duration::from_micros(100), 1, now + Duration::from_secs(1));
        let recovered = t.snapshot(1, Some(now), 6, 0, now);
        assert!(recovered.pricing_ms_per_trade < slow.pricing_ms_per_trade);
        t.reset_workload(now);
        assert!(t
            .snapshot(0, None, 0, 0, now)
            .upstream_rate_per_second
            .is_none());
    }
}
