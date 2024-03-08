use rdkafka::config::FromClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::ClientConfig;
use std::cmp::min;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info, instrument, warn};

use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;
use crate::trade::{TradeReduce, TradeRep};
use uuid::Uuid;

/// sends new trades from the kafka position topic
/// to new portfolio sender and current portfolio sender.
/// if it receives a signal to resend existing trades, it resends them
pub trait PortfolioSender: TradeReduce + Streaming + Sync {
    //#[instrument]
    fn __construct_portfolio(
        &self,
        sender_new: Sender<<Self as TradeReduce>::ReductionType>,
        sender_curr: Sender<<Self as TradeReduce>::ReductionType>,
        resend_existing: Receiver<bool>,
        pos_topic: String,
    ) -> impl Future<Output = ()> + Send
    where
        for<'a> <Self as TradeReduce>::TradeType: std::fmt::Debug + TryFromRef<BorrowedMessage<'a>>,
    {
        async move {
            let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port(),);
            let position_listener = connect_with_retries_rd(&bootstrap_servers, &pos_topic);

            let existing_trades = Arc::new(Mutex::new(TradeRep::<
                <Self as TradeReduce>::ReductionType,
            >::default()));
            let existing_trades_2 = Arc::clone(&existing_trades);
            let sender_new_2 = sender_new.clone();

            tokio_scoped::scope(|scope| {
                scope.spawn(self._send_trade_fut(
                    position_listener,
                    sender_new,
                    sender_curr,
                    existing_trades,
                ));

                scope.spawn(self._resend_value(resend_existing, existing_trades_2, sender_new_2));
            });
        }
    }

    fn _send_trade_fut(
        &self,
        position_listener: StreamConsumer,
        sender_new: Sender<<Self as TradeReduce>::ReductionType>,
        sender_curr: Sender<<Self as TradeReduce>::ReductionType>,
        existing_trades: Arc<Mutex<TradeRep<<Self as TradeReduce>::ReductionType>>>,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        for<'a> <Self as TradeReduce>::TradeType: std::fmt::Debug + TryFromRef<BorrowedMessage<'a>>,
    {
        async move {
            loop {
                let trade = position_listener.recv().await;
                debug!("Got trade: {:?}", trade);
                let message = trade.unwrap(); // TODO: FIX UNWRAP
                match <Self as TradeReduce>::TradeType::try_from_ref(&message) {
                    Err(e) => {
                        warn!("Problem w/ trade: {:?}", e);
                    }
                    Ok(trade) => {
                        debug!("Sending trade {:?} to CURR & NEW processor.", &trade);

                        // add trades to trade_reduce
                        let tr = self.reduce(&trade);
                        let _ = sender_new.send(tr.clone()).await;
                        let _ = sender_curr.send(tr.clone()).await;
                        *existing_trades.lock().unwrap() += &tr;
                    }
                }
                match position_listener.commit_message(&message, CommitMode::Async) {
                    Ok(_) => {
                        debug!("Successful commit of message!");
                    }
                    Err(e) => {
                        error!("Something wrong with {:?}: {:?}", message, e);
                    }
                }
            }
        }
    }

    /// future handling the resending of the trades.
    ///   resend_existing: channel whether to resend.
    fn _resend_value(
        &self,
        mut resend_existing: Receiver<bool>,
        existing_trades_2: Arc<Mutex<TradeRep<<Self as TradeReduce>::ReductionType>>>,
        sender_new_2: Sender<<Self as TradeReduce>::ReductionType>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                let resend = resend_existing.recv().await;
                match resend {
                    Some(resend_val) => {
                        debug!("Got a resend value {}", resend_val);
                        if resend_val {
                            // fill sender_new with existing trades
                            let curr_trades = existing_trades_2.lock().unwrap().clone();
                            for (_tid, trade) in curr_trades.iter() {
                                let _ = sender_new_2.send(trade.clone()).await;
                            }
                        }
                    }
                    None => {
                        error!("Resend channel problems. This should NOT happen.");
                    }
                }
            }
        }
    }
}

impl<T> PortfolioSender for T
where
    T: Streaming + TradeReduce + std::fmt::Debug + Sync,
    for<'a> <T as TradeReduce>::TradeType: TryFromRef<BorrowedMessage<'a>> + std::fmt::Debug,
{
}

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
