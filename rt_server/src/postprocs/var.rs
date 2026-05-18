use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

use crate::portfolio_sender::connect_with_retries_rd;
use crate::publish::connect_with_retries_producer_rd;

#[derive(Debug, Clone, Deserialize)]
pub struct RiskResultMessage {
    #[serde(rename = "PV", default)]
    pub pv: HashMap<String, f64>,
    #[serde(default)]
    pub delta: HashMap<String, f64>,
    #[serde(default)]
    pub vol: HashMap<String, f64>,
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

pub struct VarProcessor {
    consumer: StreamConsumer,
    producer: FutureProducer,
    topic: String,
    latest_pvs: HashMap<String, f64>,
    latest_deltas: HashMap<String, f64>,
    latest_vols: HashMap<String, f64>,
    confidence: f64,
}

trait Aggregator {
    fn aggregate(
        &self,
        deltas: &HashMap<String, f64>,
        vols: &HashMap<String, f64>,
    ) -> f64;
}

impl Aggregator for VarProcessor {
    fn aggregate(
        &self,
        deltas: &HashMap<String, f64>,
        vols: &HashMap<String, f64>,
    ) -> f64 {
        let variance: f64 = deltas
            .iter()
            .map(|(factor, delta)| {
                let vol = vols.get(factor).copied().unwrap_or(0.0);
                let shock = delta * vol;
                shock * shock
            })
            .sum();

        z_score_from_confidence(self.confidence) * variance.sqrt()
    }
}

impl VarProcessor {
    pub fn new(
        bootstrap_servers: &str,
        topic: &str,
        group_id: &str,
    ) -> anyhow::Result<Self> {
        Self::new_with_config(bootstrap_servers, topic, group_id, 0.95)
    }

    pub fn new_with_config(
        bootstrap_servers: &str,
        topic: &str,
        _group_id: &str,
        confidence: f64,
    ) -> anyhow::Result<Self> {
        let consumer = connect_with_retries_rd(bootstrap_servers, topic);
        let producer = connect_with_retries_producer_rd(bootstrap_servers);

        Ok(Self {
            consumer,
            producer,
            topic: topic.to_string(),
            latest_pvs: HashMap::new(),
            latest_deltas: HashMap::new(),
            latest_vols: HashMap::new(),
            confidence,
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

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!(
            "Starting VaR processor on topic {} with confidence {}",
            self.topic, self.confidence
        );

        loop {
            let msg = self.consumer.recv().await?;

            let Some(payload) = msg.payload() else {
                warn!("Received Kafka message without payload");
                continue;
            };

            let Ok(parsed) = serde_json::from_slice::<RiskResultMessage>(payload) else {
                error!("Could not deserialize risk result payload.");
                continue;
            };

            for (trade_id, trade_value) in parsed.pv {
                self.latest_pvs.insert(trade_id, trade_value);
            }

            for (factor, delta) in parsed.delta {
                self.latest_deltas.insert(factor, delta);
            }

            for (factor, vol) in parsed.vol {
                self.latest_vols.insert(factor, vol);
            }

            if self.latest_deltas.is_empty() || self.latest_vols.is_empty() {
                warn!("No delta/vol inputs available for VaR. Publishing 0.0.");
            }

            let total_var = self.aggregate(&self.latest_deltas, &self.latest_vols);

            let var_msg = VarResultMessage {
                id: "TOTAL_VAR".to_string(),
                metric: "VaR".to_string(),
                value: total_var,
                kind: "var".to_string(),
                ts_ms: now_ms(),
                confidence: self.confidence,
                methodology: "delta_normal_diagonal".to_string(),
            };

            let payload = serde_json::to_string(&var_msg)?;

            let record = FutureRecord::to(&self.topic)
                .key(&var_msg.id)
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
