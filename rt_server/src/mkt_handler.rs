use log::debug;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, Message, };

use std::sync::mpsc::Sender;

use crate::streaming::Streaming;
use crate::market::{MarketType, MktMsgParams,};


pub trait MktEventHandler : Streaming {

    fn _handle_mkt_msg(
        &self,
        mkt_msg: &Message,
        new_mkt_sender: Sender<MarketType>,
        mkt_params: MktMsgParams,
    );

    /// Loop that handles the market events
    /// mkt_topic - receiving market events from this topic
    /// new_mkt_sender - sending the new market to the pricing api
    /// switch_mkt_recv - receiver receiving the event when to switch markets.
    fn _handle_mkt_events (
        &self,
        mkt_topic: String,
        mkt_params: MktMsgParams,
        new_mkt_sender: Sender<MarketType>,
    ) {
        let mut mkt_listener_ = Consumer::from_hosts(vec![format!(
            "{}:{}",
            self.kafka_server_name(), self.kafka_port()
        )])
        .with_topic_partitions(mkt_topic.to_owned(), &[0])
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

        debug!("_handle_mkt_events: Entering the _handle_mkt_events loop.");
        loop {
            for mkt_msg_set in mkt_listener_.poll().unwrap().iter() {  // TODO: What to do w/ unwrap here??
                for mkt_msg in mkt_msg_set.messages() {
                    debug!("_handle_mkt_events: Getting new markets from {mkt_topic}.");
                    self._handle_mkt_msg(mkt_msg, new_mkt_sender.clone(), mkt_params.clone());
             }
                let _ = mkt_listener_.consume_messageset(mkt_msg_set);
            }
            mkt_listener_.commit_consumed().unwrap();
        }
    }
}
