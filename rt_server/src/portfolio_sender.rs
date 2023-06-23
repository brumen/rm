use log::{warn, debug,};
use std::sync::mpsc::Sender;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, };

use crate::ref_deref::TryFromRef;
use crate::trade::TradeAggregation;
use crate::streaming::Streaming;


pub trait PortfolioSender : TradeAggregation
{
    fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TT>,
        sender_curr: Sender<Self::TT>,
        pos_topic: String,
    );
}


impl<T> PortfolioSender for T
where
    T: Streaming + TradeAggregation,
    //TT: for<'a> TryFromRef<Message<'a>> + std::fmt::Debug + Send + Clone,
    //for<'a> <TT as TryFromRef<Message<'a>>>::Error: Debug,
{
    fn __construct_portfolio(
        &self,
        sender_new: Sender<Self::TT>,
        sender_curr: Sender<Self::TT>,
        pos_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());

        let mut pos_listener = Consumer::from_hosts(vec![bootstrap_servers,])
            .with_topic_partitions(pos_topic, &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();

        loop {
            for ms in pos_listener.poll().unwrap().iter() {
                debug!("__construct_portfolio: got some messages");
                for msg in ms.messages() {
                    debug!("__construct_portfolio: {:?}",  msg);

                    match Self::TT::try_from_ref(msg) {
                        Err(e) => {
                            warn!("__construct_portfolio: Problem w/ trade: {:?}", e);
                            continue;
                        },
                        Ok(trade) => {
                            debug!("__construct_portfolio: sending trade {:?}", trade);
                            let _ = sender_new.send(trade.clone());
                            let _ = sender_curr.send(trade);
                        },
                    }
                }
                let _ = pos_listener.consume_messageset(ms); // TODO: FIX THIS ERROR HANDLING HERE
            }
            pos_listener.commit_consumed().unwrap();
        }
    }
}
