use rdkafka::config::FromClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::ClientConfig;
use std::cmp::min;
use std::thread::sleep;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// attempts to connect the RDKafka consumer to Kafka
///  if it cant, returns the KafkaErr TODO: TO BE CHANGED.
pub fn connect_with_retries_rd(bootstrap_servers: &str, pos_topic: &str) -> StreamConsumer {
    let mut current_sleep_time = 1;

    let mut pos_consumer_config = ClientConfig::new();
    pos_consumer_config.set("bootstrap.servers", bootstrap_servers);
    pos_consumer_config.set("group.id", Uuid::new_v4().to_string());

    info!(
        "Attempting to connect to {:?} on topic {:?}",
        bootstrap_servers, pos_topic,
    );

    loop {
        // .set("enable.partition.eof", "false")
        // We'll give each session its own (unique) consumer group id,
        // so that each session will receive all messages

        match StreamConsumer::from_config(&pos_consumer_config) {
            Ok(pos_listener) => {
                pos_listener
                    .subscribe(&[pos_topic])
                    .expect("Cant subscribe to topic");
                debug!("Connected to {:?} on {:?}", bootstrap_servers, pos_topic);
                return pos_listener;
            }
            Err(e) => {
                warn!(
                    "listener is not connected, waiting {:?} secs: {:?}",
                    current_sleep_time, e,
                );
                sleep(Duration::new(current_sleep_time, 0));
                current_sleep_time = min(current_sleep_time + 1, 5);
            }
        };
    }
}

/// connects the consumer to Kafka,
/// keep retyring every 5 seconds.
#[allow(dead_code)]
pub fn connect_with_retries(bootstrap_servers: &str, pos_topic: &str) -> kafka::consumer::Consumer {
    let mut current_sleep_time = 1;

    loop {
        match kafka::consumer::Consumer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_topic_partitions(pos_topic.to_owned(), &[0])
            .with_fallback_offset(kafka::consumer::FetchOffset::Earliest)
            .with_offset_storage(kafka::consumer::GroupOffsetStorage::Kafka)
            .create()
        {
            Ok(pos_listener) => {
                return pos_listener;
            }
            Err(e) => {
                warn!(
                    "listener is not connected, waiting {:?} secs: {:?}",
                    current_sleep_time, e
                );
                sleep(Duration::new(current_sleep_time, 0));
                current_sleep_time += min(current_sleep_time + 1, 5);
            }
        };
    }
}
