use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::portfolio_sender::connect_with_retries_rd;
use crate::publish::connect_with_retries_producer_rd;

const DEFAULT_MARKET_RAW_TOPIC: &str = "letf.mkt_raw";
const DEFAULT_RETURN_WINDOW: usize = 250;
const DEFAULT_MIN_OBSERVATIONS: usize = 20;

#[derive(Debug, Clone, Deserialize)]
pub struct RiskResultMessage {
    #[serde(rename = "PV", default)]
    pub pv: HashMap<String, f64>,

    // Legacy optional flat delta format.
    #[serde(default)]
    pub delta: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VarResultMessage {
    pub id: String,
    pub metric: String,
    pub value: f64,
    pub kind: String,
    pub ts_ms: u64,
    pub confidence: f64,
    pub methodology: String,
}

enum VarInputMessage {
    Risk {
        key: Option<String>,
        payload: Vec<u8>,
    },
    Market {
        key: Option<String>,
        payload: Vec<u8>,
    },
}

pub struct VarProcessor {
    risk_consumer: StreamConsumer,
    market_consumer: StreamConsumer,
    producer: FutureProducer,

    risk_topic: String,
    market_raw_topic: String,

    latest_pvs: HashMap<String, f64>,

    // Aggregated portfolio exposure by market factor/asset.
    // Usually populated from PV01 messages keyed by "PV01".
    latest_exposures: HashMap<String, f64>,

    // Latest raw market prices by factor/asset.
    latest_prices: HashMap<String, f64>,

    // Rolling log-return history by factor/asset.
    returns: HashMap<String, VecDeque<f64>>,

    confidence: f64,
    return_window: usize,
    min_observations: usize,
}

trait Aggregator {
    fn aggregate(&self) -> Option<f64>;
}

impl Aggregator for VarProcessor {
    fn aggregate(&self) -> Option<f64> {
        if self.latest_exposures.is_empty() {
            return None;
        }

        let factors = self
            .latest_exposures
            .keys()
            .filter(|factor| {
                self.returns
                    .get(*factor)
                    .map(|xs| xs.len() >= self.min_observations)
                    .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();

        if factors.is_empty() {
            return None;
        }

        let mut variance = 0.0;

        for factor_i in factors.iter() {
            let exposure_i = self.latest_exposures.get(factor_i).copied().unwrap_or(0.0);

            for factor_j in factors.iter() {
                let exposure_j = self.latest_exposures.get(factor_j).copied().unwrap_or(0.0);

                let cov_ij = rolling_covariance(
                    self.returns.get(factor_i)?,
                    self.returns.get(factor_j)?,
                    self.min_observations,
                )?;

                variance += exposure_i * exposure_j * cov_ij;
            }
        }

        let variance = variance.max(0.0);
        Some(z_score_from_confidence(self.confidence) * variance.sqrt())
    }
}

impl VarProcessor {
    pub fn new(bootstrap_servers: &str, topic: &str, group_id: &str) -> anyhow::Result<Self> {
        Self::new_with_config(bootstrap_servers, topic, group_id, 0.95)
    }

    pub fn new_with_config(
        bootstrap_servers: &str,
        topic: &str,
        group_id: &str,
        confidence: f64,
    ) -> anyhow::Result<Self> {
        Self::new_with_market_config(
            bootstrap_servers,
            topic,
            group_id,
            confidence,
            DEFAULT_MARKET_RAW_TOPIC,
            DEFAULT_RETURN_WINDOW,
            DEFAULT_MIN_OBSERVATIONS,
        )
    }

    pub fn new_with_market_config(
        bootstrap_servers: &str,
        topic: &str,
        _group_id: &str,
        confidence: f64,
        market_raw_topic: &str,
        return_window: usize,
        min_observations: usize,
    ) -> anyhow::Result<Self> {
        let risk_consumer = connect_with_retries_rd(bootstrap_servers, topic);
        let market_consumer = connect_with_retries_rd(bootstrap_servers, market_raw_topic);
        let producer = connect_with_retries_producer_rd(bootstrap_servers);

        Ok(Self {
            risk_consumer,
            market_consumer,
            producer,

            risk_topic: topic.to_string(),
            market_raw_topic: market_raw_topic.to_string(),

            latest_pvs: HashMap::new(),
            latest_exposures: HashMap::new(),
            latest_prices: HashMap::new(),
            returns: HashMap::new(),

            confidence,
            return_window,
            min_observations,
        })
    }

    pub async fn run_ignore(&mut self) {
        match self.run().await {
            Ok(_) => {
                info!("All good");
            }
            Err(e) => {
                error!("Got error: {:?}", e);
            }
        }
    }

    pub async fn run_with_restart(&mut self) -> anyhow::Result<()> {
        loop {
            if let Err(e) = self.run().await {
                error!("VaR failed: {:?}. Restarting.", e);
            }
        }
    }

    fn handle_risk_payload(&mut self, msg_key: Option<&str>, payload: &[u8]) {
        match msg_key {
            Some("PV") => match serde_json::from_slice::<HashMap<String, f64>>(payload) {
                Ok(pv_updates) => {
                    for (trade_id, pv) in pv_updates {
                        self.latest_pvs.insert(trade_id, pv);
                    }
                }
                Err(e) => {
                    error!("Could not deserialize keyed PV payload: {:?}", e);
                }
            },

            Some("PV01") => match parse_exposure_payload(payload) {
                Ok(exposures) => {
                    self.latest_exposures = exposures;
                    debug!(
                        "Updated VaR exposures from PV01 payload. factors={}",
                        self.latest_exposures.len()
                    );
                }
                Err(e) => {
                    error!("Could not deserialize keyed PV01 exposure payload: {:?}", e);
                }
            },

            Some("VaR") | Some("NAV") | Some("PnL") => {
                debug!(
                    "Ignoring post-processed/non-exposure risk message with key {:?}",
                    msg_key
                );
            }

            Some(other) => {
                debug!("Ignoring unsupported risk message key {}", other);
            }

            None => {
                // Legacy compatibility.
                match serde_json::from_slice::<RiskResultMessage>(payload) {
                    Ok(parsed) => {
                        for (trade_id, pv) in parsed.pv {
                            self.latest_pvs.insert(trade_id, pv);
                        }

                        if !parsed.delta.is_empty() {
                            self.latest_exposures = parsed.delta;
                        }
                    }
                    Err(e) => {
                        error!("Could not deserialize legacy risk result payload: {:?}", e);
                    }
                }
            }
        }
    }

    fn handle_market_payload(&mut self, msg_key: Option<&str>, payload: &[u8]) {
        match parse_market_payload(msg_key, payload) {
            Ok(price_updates) => {
                if price_updates.is_empty() {
                    debug!("Received empty market update");
                    return;
                }

                for (factor, price) in price_updates {
                    self.update_price(factor, price);
                }
            }
            Err(e) => {
                error!("Could not deserialize raw market payload: {:?}", e);
            }
        }
    }

    fn update_price(&mut self, factor: String, price: f64) {
        if !price.is_finite() || price <= 0.0 {
            debug!("Ignoring invalid market price for {}: {}", factor, price);
            return;
        }

        if let Some(prev_price) = self.latest_prices.insert(factor.clone(), price) {
            if prev_price.is_finite() && prev_price > 0.0 {
                let ret = (price / prev_price).ln();

                if ret.is_finite() {
                    let series = self.returns.entry(factor).or_insert_with(VecDeque::new);

                    series.push_back(ret);

                    while series.len() > self.return_window {
                        series.pop_front();
                    }
                }
            }
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!(
            "Starting VaR processor. risk_topic={}, market_raw_topic={}, confidence={}, return_window={}, min_observations={}",
            self.risk_topic,
            self.market_raw_topic,
            self.confidence,
            self.return_window,
            self.min_observations,
        );

        loop {
            let input_msg = tokio::select! {
                risk_msg = self.risk_consumer.recv() => {
                    let msg = risk_msg?;

                    let Some(payload) = msg.payload() else {
                        warn!("Received risk Kafka message without payload");
                        continue;
                    };

                    let key = match msg.key() {
                        Some(key_bytes) => match std::str::from_utf8(key_bytes) {
                            Ok(key) => Some(key.to_string()),
                            Err(e) => {
                                warn!("Received risk Kafka message with non-UTF8 key: {:?}", e);
                                continue;
                            }
                        },
                        None => None,
                    };

                    VarInputMessage::Risk {
                        key,
                        payload: payload.to_vec(),
                    }
                }

                market_msg = self.market_consumer.recv() => {
                    let msg = market_msg?;

                    let Some(payload) = msg.payload() else {
                        warn!("Received market Kafka message without payload");
                        continue;
                    };

                    let key = match msg.key() {
                        Some(key_bytes) => match std::str::from_utf8(key_bytes) {
                            Ok(key) => Some(key.to_string()),
                            Err(e) => {
                                warn!("Received market Kafka message with non-UTF8 key: {:?}", e);
                                continue;
                            }
                        },
                        None => None,
                    };

                    VarInputMessage::Market {
                        key,
                        payload: payload.to_vec(),
                    }
                }
            };

            match input_msg {
                VarInputMessage::Risk { key, payload } => {
                    self.handle_risk_payload(key.as_deref(), &payload);
                }
                VarInputMessage::Market { key, payload } => {
                    self.handle_market_payload(key.as_deref(), &payload);
                }
            }

            let Some(total_var) = self.aggregate() else {
                debug!(
                    "Not enough exposure/market history to compute VaR yet. exposures={}, priced_factors={}",
                    self.latest_exposures.len(),
                    self.returns.len(),
                );
                continue;
            };

            let var_msg = VarResultMessage {
                id: "TOTAL_VAR".to_string(),
                metric: "VaR".to_string(),
                value: total_var,
                kind: "var".to_string(),
                ts_ms: now_ms(),
                confidence: self.confidence,
                methodology: "delta_normal_full_covariance_from_rolling_returns".to_string(),
            };

            let payload = serde_json::to_string(&var_msg)?;

            let record = FutureRecord::to(&self.risk_topic)
                .key(&var_msg.metric)
                .payload(&payload);

            match self.producer.send(record, Timeout::Never).await {
                Ok(_) => {
                    debug!("Published VaR update: {}", payload);
                }
                Err((e, _)) => {
                    error!("Failed to publish VaR update: {:?}", e);
                }
            }
        }
    }
}

fn parse_exposure_payload(payload: &[u8]) -> anyhow::Result<HashMap<String, f64>> {
    let value = serde_json::from_slice::<Value>(payload)?;

    let Some(obj) = value.as_object() else {
        anyhow::bail!("PV01/exposure payload must be a JSON object");
    };

    let mut exposures = HashMap::<String, f64>::new();

    for (outer_key, outer_value) in obj {
        if let Some(v) = outer_value.as_f64() {
            // Flat format:
            // {"SPY": 123.0, "QQQ": -45.0}
            *exposures.entry(outer_key.clone()).or_insert(0.0) += v;
            continue;
        }

        if let Some(inner_obj) = outer_value.as_object() {
            // Nested format:
            // {"trade_1": {"SPY": 123.0}, "trade_2": {"SPY": -50.0, "QQQ": 25.0}}
            for (factor, factor_value) in inner_obj {
                if let Some(v) = factor_value.as_f64() {
                    *exposures.entry(factor.clone()).or_insert(0.0) += v;
                }
            }
        }
    }

    Ok(exposures)
}

fn parse_market_payload(
    msg_key: Option<&str>,
    payload: &[u8],
) -> anyhow::Result<HashMap<String, f64>> {
    let value = serde_json::from_slice::<Value>(payload)?;
    let mut updates = HashMap::<String, f64>::new();

    collect_market_updates(msg_key, &value, &mut updates)?;

    Ok(updates)
}

fn collect_market_updates(
    msg_key: Option<&str>,
    value: &Value,
    updates: &mut HashMap<String, f64>,
) -> anyhow::Result<()> {
    match value {
        Value::Number(n) => {
            let Some(key) = msg_key else {
                anyhow::bail!("numeric market payload requires Kafka key");
            };

            if let Some(price) = n.as_f64() {
                updates.insert(key.to_string(), price);
            }
        }

        Value::Object(obj) => {
            // Map format:
            // {"SPY": 500.0, "QQQ": 420.0}
            let mut consumed_as_price_map = false;

            for (k, v) in obj {
                if let Some(price) = v.as_f64() {
                    updates.insert(k.clone(), price);
                    consumed_as_price_map = true;
                }
            }

            if !consumed_as_price_map {
                // Single Rust-enum-like factor object with separate keyed numeric payload
                // is not supported unless represented as tuple/list.
                debug!("Ignoring non-price market object payload: {:?}", value);
            }
        }

        Value::Array(arr) => {
            // Batch format:
            // [[{"Stock":"SPY"}, 500.0], [{"Stock":"QQQ"}, 420.0]]
            //
            // Single tuple format:
            // [{"Stock":"SPY"}, 500.0]
            if arr.len() == 2 && arr[1].as_f64().is_some() {
                let factor = factor_name_from_value(&arr[0]);
                let price = arr[1].as_f64().unwrap();

                updates.insert(factor, price);
            } else {
                for item in arr {
                    collect_market_updates(msg_key, item, updates)?;
                }
            }
        }

        _ => {
            debug!("Ignoring unsupported market payload: {:?}", value);
        }
    }

    Ok(())
}

fn factor_name_from_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),

        Value::Object(obj) => {
            if obj.len() == 1 {
                let (variant, inner) = obj.iter().next().unwrap();

                match inner {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Object(inner_obj) => {
                        if inner_obj.len() == 1 {
                            let (_, inner_value) = inner_obj.iter().next().unwrap();
                            match inner_value {
                                Value::String(s) => s.clone(),
                                Value::Number(n) => n.to_string(),
                                _ => value.to_string(),
                            }
                        } else {
                            value.to_string()
                        }
                    }
                    Value::Null => variant.clone(),
                    _ => value.to_string(),
                }
            } else {
                value.to_string()
            }
        }

        _ => value.to_string(),
    }
}

fn rolling_covariance(
    xs: &VecDeque<f64>,
    ys: &VecDeque<f64>,
    min_observations: usize,
) -> Option<f64> {
    let n = xs.len().min(ys.len());

    if n < min_observations || n < 2 {
        return None;
    }

    let xs_tail = xs.iter().rev().take(n).copied().collect::<Vec<_>>();
    let ys_tail = ys.iter().rev().take(n).copied().collect::<Vec<_>>();

    let mean_x = xs_tail.iter().sum::<f64>() / n as f64;
    let mean_y = ys_tail.iter().sum::<f64>() / n as f64;

    let cov = xs_tail
        .iter()
        .zip(ys_tail.iter())
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>()
        / (n - 1) as f64;

    Some(cov)
}

fn z_score_from_confidence(confidence: f64) -> f64 {
    if confidence >= 0.995 {
        2.576
    } else if confidence >= 0.99 {
        2.326
    } else if confidence >= 0.975 {
        1.960
    } else if confidence >= 0.95 {
        1.645
    } else if confidence >= 0.90 {
        1.282
    } else {
        1.0
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
