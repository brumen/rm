use rdkafka::config::FromClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::cmp::min;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, error, info, warn};

// use lapin::options::ClientOptions; // TODO:
use lapin::Connection;
use std::error::Error;

use crate::portfolio::PortfolioType;
use crate::pricer::PricingMetric;
use crate::streaming::Streaming;

/// publishes the results to the of the current portfolio
/// to the results topic.
pub trait PublishResults: Streaming
where
    Self: Sync,
{
    fn metric(&self) -> PricingMetric;

    /// publishes results to the server.
    fn _publish_results<TT: Send>(
        &self,
        mut curr_portfolio_recv: Receiver<(PortfolioType, TT)>,
        results_topic: String,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());
            let res_publisher = connect_with_retries_producer_rd(&bootstrap_servers);

            loop {
                debug!("Looping _publish_results");
                let curr_portfolio = match curr_portfolio_recv.recv().await {
                    Some((curr_portfolio_actual, _)) => {
                        debug!(
                            "_publish_results: Found actual portfolio: {:?}",
                            curr_portfolio_actual
                        );
                        curr_portfolio_actual
                    }
                    None => {
                        error!("Channel curr_portfolio_recv has been dropped. Investigate!");
                        panic!();
                    }
                };

                let curr_mkt_json = match serde_json::ser::to_string(&curr_portfolio) {
                    Err(e) => {
                        error!("Could not convert result portfolio to json: {:?}", e);
                        return;
                    }
                    Ok(curr_mkt_json) => curr_mkt_json,
                };

                let curr_mkt_pv = format!("{{\"{}\": {}}}", self.metric(), curr_mkt_json);

                // implements bytearray(str(dumps(self.curr_market)), ascii))
                // TODO: REMOVE THE NEXT 2 lines later.
                //let market_record =
                //    kafka::producer::Record::from_value(&results_topic, curr_mkt_pv.as_bytes()).with_partition(0);
                let market_record2: FutureRecord<'_, [u8], [u8]> = FutureRecord {
                    topic: &results_topic,
                    partition: Some(0),
                    payload: Some(curr_mkt_pv.as_bytes()),
                    key: None, // TODO: pub key: Option<&'a K>,
                    timestamp: None,
                    headers: None,
                };
                debug!("Publishing results: {:?}", market_record2);
                let _ = res_publisher.send(market_record2, Timeout::Never).await;
            }
        }
    }
}

/// connects the consumer to RDKafka library, retries every 5 seconds
/// to try to establish connection.
pub fn connect_with_retries_producer_rd(
    bootstrap_servers: &str,
) -> rdkafka::producer::FutureProducer {
    let mut current_sleep_time = 1; // original sleep time in seconds.

    let mut result_producer_config = ClientConfig::new();
    result_producer_config.set("bootstrap.servers", bootstrap_servers);
    result_producer_config.set("group.id", "result_producer");

    // .set("enable.partition.eof", "false")
    // We'll give each session its own (unique) consumer group id,
    // so that each session will receive all messages

    loop {
        match FutureProducer::from_config(&result_producer_config) {
            Ok(result_producer) => {
                info!("Connected to kafka producer {:?}", bootstrap_servers);
                return result_producer;
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

// pub async fn connect_with_retries_rabbitmq(uri: &str) -> Result<Connection, Box<dyn Error>> {
//     let mut current_sleep_time = 1; // original sleep time in seconds

//     loop {
//         let connection = Connection::connect(uri, ClientOptions::default()).await;

//         match connection {
//             Ok(conn) => {
//                 info!("Connected to RabbitMQ at {:?}", uri);
//                 return Ok(conn);
//             }
//             Err(e) => {
//                 warn!(
//                     "Listener is not connected, waiting {:?} secs: {:?}",
//                     current_sleep_time, e,
//                 );
//                 sleep(Duration::new(current_sleep_time, 0));
//                 current_sleep_time = min(current_sleep_time + 1, 5);
//             }
//         };
//     }
// }

/// connects the consumer to Kafka, retries every 5 seconds
/// to try to establish connection.
pub fn connect_with_retries_producer(bootstrap_servers: &str) -> kafka::producer::Producer {
    let mut sleep_duration = 1;

    loop {
        match kafka::producer::Producer::from_hosts(vec![bootstrap_servers.to_owned()])
            .with_required_acks(kafka::producer::RequiredAcks::One)
            .create()
        {
            Ok(pos_listener) => {
                info!("Connected to kafka producer {:?}", bootstrap_servers);
                return pos_listener; // maybe Some missing here
            }
            Err(e) => {
                warn!(
                    "__construct_portfolio: listener is not connected, waiting {:?} secs: {:?}",
                    sleep_duration, e,
                );
                sleep(Duration::new(sleep_duration, 0));
                sleep_duration = min(sleep_duration + 1, 5);
            }
        };
    }
}
