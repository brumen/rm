use log::{warn, debug, info, };
use std::sync::mpsc::{Sender, Receiver,};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, };
use std::thread::sleep;
use std::time::Duration;

use crate::ref_deref::TryFromRef;
use crate::streaming::Streaming;
use crate::trade::{
    TradeReduce,
    TradeRep,
    BaseTrade,
};


pub trait PortfolioSender : TradeReduce
{
    /// sends new trades from the kafka position topic
    /// to new portfolio sender and current portfolio sender.
    /// if it receives a signal to resend existing trades, it resends them

    type TR : Clone + Send + BaseTrade;

    // fn reduce(&self, trade: Message<'_>) -> (String, Self::TR);

    fn __construct_portfolio(
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
    for <'a> <T as TradeReduce>::TradeType: TryFromRef<Message<'a>> + std::fmt::Debug
//    TT: for<'a> TryFromRef<Message<'a>> + std::fmt::Debug + Send + Clone + BaseTrade,
//    for<'a> <TT as TryFromRef<Message<'a>>>::Error: std::fmt::Debug,
{
    type TR = <T as TradeReduce>::ReductionType;

    fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TR>,
        sender_curr: Sender<Self::TR>,
        resend_existing: Receiver<bool>,
        pos_topic: String,
    ) {
        let bootstrap_servers = format!(
            "{}:{}",
            self.kafka_server_name(),
            self.kafka_port(),
        );
	    let mut position_listener = connect_with_retries(
            &bootstrap_servers,
            &pos_topic,
        );

        let mut existing_trades = TradeRep::<Self::TR>::new();

        loop {

            let resend = resend_existing.try_recv();  // should we resend existing trades
            match resend {
                Ok(resend_val) => {
                    info!("_construct_portfolio: Got a resend value {}", resend_val);
                    if resend_val {
                        // fill sender_new with existing trades
                        for (tid, trade) in existing_trades.iter() {
                            // TODO: THIS IS SHITTY - TRY TO IMPLEMENT THIS WITHOUT CLONING
                            let _ = sender_new.send(trade.clone());
                        }
                    }
                },
                Err(tre) => {
                    debug!("_construct_portfolio: resend channel problems: {:?}", tre);
                },
            }

            for ms in position_listener.poll().unwrap().iter() {
                for msg in ms.messages() {
                    match <T as TradeReduce>::TradeType::try_from_ref(msg) {
                        Err(e) => {
                            warn!("__construct_portfolio: Problem w/ trade: {:?}", e);
                            continue;
                        },
                        Ok(trade) => {
                            info!("__construct_portfolio: sending trade {:?}", trade);

                            // add trades to trade_reduce
                            let tr = self.reduce(&trade);
                            let _ = sender_new.send(tr.clone());
                            let _ = sender_curr.send(tr.clone());
                            existing_trades += &tr;
                        },
                    }
                }
                let _ = position_listener.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            position_listener.commit_consumed().unwrap();
        }
    }
}


/// connects the consumer to Kafka
pub fn connect_with_retries(bootstrap_servers: &str, pos_topic: &str) -> Consumer {
    let mut listener_connected = false;
    let mut eventual_listener = None;

    while !listener_connected {
	    match Consumer::from_hosts(vec![bootstrap_servers.to_owned(),])
	        .with_topic_partitions(pos_topic.to_owned(), &[0])
	        .with_fallback_offset(FetchOffset::Earliest)
	        .with_offset_storage(GroupOffsetStorage::Kafka)
	        .create() {
		        Ok(pos_listener) => {
		            eventual_listener = Some(pos_listener);
		            listener_connected = true;
		        },
		        Err(e) => {
		            warn!("__construct_portfolio: listener is not connected, waiting 5 secs: {:?}", e);
		            sleep(Duration::new(5, 0));
		            eventual_listener = None;
		        },
	        };
    }
    eventual_listener.unwrap()
}
