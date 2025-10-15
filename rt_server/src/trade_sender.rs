//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use serde::Deserialize;
use tracing::{info, instrument};
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::portfolio_sender::connect_with_retries_rd;
use crate::ref_deref::{TryFromRef, TryFromRef2,};
use crate::trade::{BaseTrade, TradeRep};

// new and current processors.
use crate::processor_msg::ProcessorMiddleMessage;

pub struct TradeProducer<'a, T>{
    position_listener: StreamConsumer,
    processors: Vec<ActorRef<ProcessorMiddleMessage<T>>>,
    trade_list: &'a TradeRep<T>,

}

impl<'a, T> TradeProducer<'a, T> {
    pub fn new(
	kafka_server: String,
	pos_topic: String,
	processors: Vec<ActorRef<ProcessorMiddleMessage<T>>>,
        trade_list: &'a TradeRep<T>,
    ) -> Self {

	info!("Starting trade producer on {:?}", pos_topic);
	let position_listener = connect_with_retries_rd(&kafka_server, &pos_topic);

	Self {
	    position_listener,
	    processors,
            trade_list,
	}
    }
}


#[async_trait]
impl<'b, T> Actor for TradeProducer<'b, T>
where
    TradeProducer<'b, T>: Send + Sync + 'static,
    T: Send + Sync + std::fmt::Debug + Clone + BaseTrade + for <'a> Deserialize<'a> + TryFromRef2
{
    type Msg = ProcessorMiddleMessage<T>;
    type State = ();  // TradeRep<T>;  // list of existing trades.
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initiating TradeProducer");

        let trade_msg = self.position_listener.recv().await?;
        let trade_1 = T::try_from_ref(&trade_msg)?;

        info!("First trade: {:?}", trade_1);
        myself.send_message(
            ProcessorMiddleMessage::NewTrade(trade_1)
        )?;  // first message

        Ok(())  // Ok(TradeRep::default()))  // default empty state.
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	_state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {

        // add trades to trade_reduce
        let trade_m = message;

	for processor in &self.processors[..] {
	    processor.send_message(trade_m.clone())?;
	}

        // if let ProcessorMiddleMessage::NewTrade(trade) = trade_m {
        //     *state += &TradeRep::from([trade.clone(),]);
        // }  // only this is possible, so it's fine.

	let new_msg = self.position_listener.recv().await?;
        let new_trade = T::try_from_ref(&new_msg)?;

	myself.send_message(
            ProcessorMiddleMessage::NewTrade(new_trade)
        )?;

	Ok(())
    }
}
