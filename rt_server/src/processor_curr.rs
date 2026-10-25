//! The current processor uses the shared adaptive scheduler. Pricing and Kafka
//! delivery never run inside its message handler.
use crate::portfolio::PmPortfolio;
use ractor::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::sync::Arc;

pub(crate) use crate::processor_flow::Processor as ProcessorCurr;

#[derive(Debug, PartialEq, Clone)]
pub enum RTOperatingMode {
    DoubleBuffer,
    SingleBuffer,
}

pub(crate) const NTP_ALLOW_BEHIND: usize = 10;

#[async_trait]
pub(crate) trait PortfolioPublisher: Send + Sync + 'static {
    async fn publish(&self, portfolio: PmPortfolio) -> Result<(), String>;
}

pub(crate) struct KafkaPublisher {
    pub producer: FutureProducer,
    pub topic: String,
}

#[async_trait]
impl PortfolioPublisher for KafkaPublisher {
    async fn publish(&self, portfolio: PmPortfolio) -> Result<(), String> {
        for (metric, values) in portfolio.iter() {
            let json = serde_json::to_string(values).map_err(|e| e.to_string())?;
            let record = FutureRecord::to(&self.topic).key(metric).payload(&json);
            self.producer
                .send(
                    record,
                    Timeout::After(std::time::Duration::from_millis(100)),
                )
                .await
                .map_err(|(e, _)| e.to_string())?;
        }
        Ok(())
    }
}

pub(crate) type SharedPublisher = Arc<dyn PortfolioPublisher>;
