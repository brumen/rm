use kafka;  // ::{Consumer, FetchOffset, GroupOffsetStorage, Message};
use tracing::{debug, info, warn, instrument};
use rdkafka::consumer::Consumer;
use tokio::sync::mpsc::{Sender, Receiver};
use std::thread::sleep;
use std::time::Duration;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;
use rdkafka::message::BorrowedMessage;
use std::cmp::min;
use std::future::Future;

use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;
use crate::trade::{BaseTrade, TradeReduce, TradeRep};

pub trait PortfolioSender: TradeReduce {
    /// sends new trades from the kafka position topic
    /// to new portfolio sender and current portfolio sender.
    /// if it receives a signal to resend existing trades, it resends them

    type TR: Clone + Send + BaseTrade;

    fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TR>,
        sender_curr: Sender<Self::TR>,
        resend_existing: Receiver<bool>,
        pos_topic: String,
    ) -> impl Future<Output=()> + Send;
}

impl<T> PortfolioSender for T
where
    T: Streaming + TradeReduce + std::fmt::Debug + Sync,
    for<'a> <T as TradeReduce>::TradeType: TryFromRef<BorrowedMessage<'a>> + std::fmt::Debug
{
    type TR = <T as TradeReduce>::ReductionType;

    //#[instrument]
    fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TR>,
        sender_curr: Sender<Self::TR>,
        mut resend_existing: Receiver<bool>,
        pos_topic: String,
    ) -> impl Future<Output = ()> + Send {
        async move {
            let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port(),);
            let position_listener = connect_with_retries_rd(&bootstrap_servers, &pos_topic);

            let mut existing_trades = TradeRep::<Self::TR>::default();

	        loop {
                debug!("LOOPING POSITION LISTENER");
	            tokio::select! {
                    trade = position_listener.recv() => {
                        debug!("Got trade: {:?}", trade);
		                match <T as TradeReduce>::TradeType::try_from_ref(&trade.unwrap()) {  // TODO: FIX THIS UNWRAP
			                Err(e) => {
			                    warn!("Problem w/ trade: {:?}", e);
			                }
			                Ok(trade) => {
			                    debug!("Sending trade {:?} to CURR & NEW processor.", &trade);

			                    // add trades to trade_reduce
			                    let tr = self.reduce(&trade);
			                    let _ = sender_new.send(tr.clone()).await;
			                    let _ = sender_curr.send(tr.clone()).await;
			                    existing_trades += &tr;
			                }
		                }
		            },

		            resend = resend_existing.recv() => {
                        info!("Resending all ({:?}) trades to NEW processor.", existing_trades.len());
		                match resend {
			                Some(resend_val) => {
			                    info!("Got a resend value {}", resend_val);
			                    if resend_val {
				                    // fill sender_new with existing trades
				                    for (_tid, trade) in existing_trades.iter() {
				                        // TODO: THIS IS SHITTY - TRY TO IMPLEMENT THIS WITHOUT CLONING
				                        let _ = sender_new.send(trade.clone()).await;
				                    }
			                    }
			                },
			                None => {
			                    debug!("Resend channel problems.");
			                }
		                }
		            },
	            }
	        }
        }
    }
}


/// attempts to connect the RDKafka consumer to Kafka
///  if it cant, returns the KafkaErr TODO: TO BE CHANGED.
pub fn connect_with_retries_rd(bootstrap_servers: &str, pos_topic: &str) -> StreamConsumer {

    let mut current_sleep_time = 1;

    let mut pos_consumer_config = ClientConfig::new();
    pos_consumer_config.set("bootstrap.servers", bootstrap_servers);
    pos_consumer_config.set("group.id", "pos_listener");

    info!(
        "Attempting to connect to {:?} on topic {:?}",
        bootstrap_servers,
        pos_topic,
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
                info!("Connected to {:?} on {:?}", bootstrap_servers, pos_topic);
                return pos_listener;
            },
            Err(e) => {
                warn!(
                    "listener is not connected, waiting {:?} secs: {:?}",
		            current_sleep_time,
                    e,
                );
                sleep(Duration::new(current_sleep_time, 0));
		        current_sleep_time = min(current_sleep_time+1, 5);
            }
        };
    }
}


/// connects the consumer to Kafka,
/// keep retyring every 5 seconds.
#[allow(dead_code)]
pub fn connect_with_retries(
    bootstrap_servers: &str,
    pos_topic: &str,
) -> kafka::consumer::Consumer {

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
		    current_sleep_time,
                    e
                );
                sleep(Duration::new(current_sleep_time, 0));
		current_sleep_time += min(current_sleep_time+1, 5);
            }
        };
    }
}
