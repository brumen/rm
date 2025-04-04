//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use rdkafka::message::BorrowedMessage;
use tracing::{info, instrument};
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::portfolio_sender::connect_with_retries_rd;
use crate::ref_deref::{TryFromRef, TryFromRef2,};
use crate::trade::{BaseTrade, TradeRep};
use crate::ao_trade::AOTrade;

// new and current processors.
use crate::processor_msg::ProcessorMiddleMessage;

pub struct TradeProducer<T>{
    position_listener: StreamConsumer,
    processors: Vec<ActorRef<ProcessorMiddleMessage<T>>>,
}


impl<T> TradeProducer<T> {
    pub fn new(
	kafka_server: String,
	pos_topic: String,
	processors: Vec<ActorRef<ProcessorMiddleMessage<T>>>,
    ) -> Self {

	info!("Starting trade producer on {:?}", pos_topic);
	let position_listener = connect_with_retries_rd(&kafka_server, &pos_topic);

	Self {
	    position_listener,
	    processors,
	}
    }
}


#[async_trait]
impl<T> Actor for TradeProducer<T>
where
    T: Send + Sync + std::fmt::Debug + Clone + BaseTrade + 'static + for<'a> TryFromRef2<BorrowedMessage<'a>>,
{
    type Msg = ProcessorMiddleMessage<T>;
    type State = TradeRep<T>;  // list of existing trades.
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initiating TradeProducer");

        let msg1 = self.position_listener.recv();
        let trade_msg = msg1.await?;

        //let trade_1 = AOTrade::try_from_ref(&trade_msg)?;
        let _trade_1 = T::try_from_ref(&trade_msg)?;

        //info!("First trade: {:?}", trade_1);
        //myself.send_message(
        //    ProcessorMiddleMessage::NewTrade(trade_1)
        //)?;  // first message

        Ok(TradeRep::default())  // default empty state.
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {

        // add trades to trade_reduce
        let trade_m = message;

	for processor in &self.processors[..] {
	    processor.send_message(trade_m)?;
	}


        if let ProcessorMiddleMessage::NewTrade(trade) = trade_m {
            *state += &TradeRep::from([trade,]);
        }  // only this is possible, so it's fine.

	let new_msg = self.position_listener.recv().await?;

        let new_trade = T::try_from_ref(new_msg)?;
	myself.send_message(
            ProcessorMiddleMessage::NewTrade(new_trade)
        )?;

	Ok(())
    }
}
