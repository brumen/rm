use kafka;  // ::{Consumer, FetchOffset, GroupOffsetStorage, Message};
use log::{debug, info, warn};
use tokio::sync::mpsc::{Sender, Receiver};
use std::thread::sleep;
use std::time::Duration;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;
use rdkafka::message::BorrowedMessage;
use std::cmp::min;

use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;
use crate::trade::{BaseTrade, TradeReduce, TradeRep};

pub trait PortfolioSender: TradeReduce {
    /// sends new trades from the kafka position topic
    /// to new portfolio sender and current portfolio sender.
    /// if it receives a signal to resend existing trades, it resends them

    type TR: Clone + Send + BaseTrade;

    async fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TR>,
        sender_curr: Sender<Self::TR>,
        resend_existing: Receiver<bool>,
        pos_topic: String,
    );
}

impl<T> PortfolioSender for T
where
    T: Streaming + TradeReduce,
    for<'a> <T as TradeReduce>::TradeType: TryFromRef<BorrowedMessage<'a>> + std::fmt::Debug
{
    type TR = <T as TradeReduce>::ReductionType;

    async fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TR>,
        sender_curr: Sender<Self::TR>,
        mut resend_existing: Receiver<bool>,
        pos_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port(),);
        let position_listener = connect_with_retries_rd(&bootstrap_servers, &pos_topic);

        let mut existing_trades = TradeRep::<Self::TR>::new();

	loop {
	    tokio::select! {
		trade = position_listener.recv() => {
		    match <T as TradeReduce>::TradeType::try_from_ref(&trade.unwrap()) {  // TODO: FIX THIS UNWRAP
			Err(e) => {
			    warn!("__construct_portfolio: Problem w/ trade: {:?}", e);
			    // TODO: IS THERE ANYTHING ELSE TO DO??
			}
			Ok(trade) => {
			    info!("__construct_portfolio: sending trade {:?}", trade);
			    
			    // add trades to trade_reduce
			    let tr = self.reduce(&trade);
			    let _ = sender_new.send(tr.clone());
			    let _ = sender_curr.send(tr.clone());
			    existing_trades += &tr;
			}
		    }
		},

		resend = resend_existing.recv() => {
		    match resend {
			Some(resend_val) => {
			    info!("_construct_portfolio: Got a resend value {}", resend_val);
			    if resend_val {
				// fill sender_new with existing trades
				for (_tid, trade) in existing_trades.iter() {
				    // TODO: THIS IS SHITTY - TRY TO IMPLEMENT THIS WITHOUT CLONING
				    let _ = sender_new.send(trade.clone());
				}
			    }
			},
			None => {
			    debug!("_construct_portfolio: resend channel problems.");
			}
		    }
		},
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
    pos_consumer_config.set("topic", pos_topic);  // TODO: CHECK THIS PART

    loop {
        // .set("enable.partition.eof", "false")
        // We'll give each session its own (unique) consumer group id,
        // so that each session will receive all messages
	//            .set("group.id", format!("chat-{}", Uuid::new_v4()))

	match StreamConsumer::from_config(&pos_consumer_config) {
            Ok(pos_listener) => {
		return pos_listener;
            },
            Err(e) => {
                warn!(
                    "__construct_portfolio: listener is not connected, waiting {:?} secs: {:?}",
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
                    "__construct_portfolio: listener is not connected, waiting {:?} secs: {:?}",
		    current_sleep_time,
                    e
                );
                sleep(Duration::new(current_sleep_time, 0));
		current_sleep_time += min(current_sleep_time+1, 5);
            }
        };
    }
}
