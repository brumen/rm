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
    #[serde(rename = "PV")]
    pub pv: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NavResultMessage {
    pub id: String,
    pub metric: String,
    pub value: f64,
    pub kind: String,
    pub ts_ms: u64,
}

pub struct NavProcessor {
    consumer: StreamConsumer,
    producer: FutureProducer,
    topic: String,
    latest_pvs: HashMap<String, f64>,
}

impl NavProcessor {
    pub fn new(bootstrap_servers: &str, topic: &str, _group_id: &str) -> anyhow::Result<Self> {
        let consumer = connect_with_retries_rd(bootstrap_servers, topic);
        let producer = connect_with_retries_producer_rd(bootstrap_servers);

        Ok(Self {
            consumer,
            producer,
            topic: topic.to_string(),
            latest_pvs: HashMap::new(),
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

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("Starting NAV processor on topic {}", self.topic);

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

            let total_nav: f64 = self.latest_pvs.values().copied().sum();

            let nav_msg = NavResultMessage {
                id: "TOTAL_NAV".to_string(),
                metric: "NAV".to_string(),
                value: total_nav,
                kind: "nav".to_string(),
                ts_ms: now_ms(),
            };

            let payload = serde_json::to_string(&nav_msg)?;

            let record = FutureRecord::to(&self.topic)
                .key(&nav_msg.id)
                .payload(&payload);

            match self.producer.send(record, Timeout::Never).await {
                Ok(_) => {
                    debug!("Published NAV update: {}", payload);
                }
                Err((e, _)) => {
                    error!("Failed to publish NAV update: {:?}", e);
                }
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
