//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use serde::Deserialize;
use tracing::{info, instrument};
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use circular_buffer::CircularBuffer;

use crate::portfolio_sender::connect_with_retries_rd;
use crate::ref_deref::{TryFromRef, TryFromRef2,};
use crate::trade::{BaseTrade, TradeRep};

// new and current processors.
use crate::processor_msg::ProcessorMiddleMessage;

const CB_LENGTH: usize = 10;


pub struct TradeProducer<'a, T>{
    position_listener: StreamConsumer,
    processors: Vec<ActorRef<ProcessorMiddleMessage>>,
    trade_list: &'a TradeRep<T>,
    processing_stat: Vec<CircularBuffer<CB_LENGTH, (chrono::NaiveDateTime, usize)>>,
}

impl<'a, T> TradeProducer<'a, T> {
    pub fn new(
	kafka_server: String,
	pos_topic: String,
	processors: Vec<ActorRef<ProcessorMiddleMessage>>,
        trade_list: &'a TradeRep<T>,
    ) -> Self {

	info!("Starting trade producer on {:?}", pos_topic);
	let position_listener = connect_with_retries_rd(&kafka_server, &pos_topic);
        let nb_processors = processors.len();
        let mut processing_queue = vec![];
        for idx in 0..nb_processors {
            processing_queue.push(CircularBuffer::<CB_LENGTH, usize>::new());
        }

	Self {
	    position_listener,
	    processors,
            trade_list,
            processing_stat: processing_queue,
	}
    }
}


#[async_trait]
impl<'b, T> Actor for TradeProducer<'b, T>
where
    TradeProducer<'b, T>: Send + Sync + 'static,
    T: Send + Sync + std::fmt::Debug + Clone + BaseTrade + for <'a> Deserialize<'a> + TryFromRef2
{
    type Msg = ProcessorMiddleMessage;
    type State = ();  // TradeRep<T>;  // list of existing trades.
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initiating TradeProducer");

        // starting w/ the first trade.
        let trade_msg = self.position_listener.recv().await?;
        let trade_1 = T::try_from_ref(&trade_msg)?;

        info!("First trade: {:?}", trade_1);
        myself.send_message(
            ProcessorMiddleMessage::NewTrade(trade_1)
        )?;  // first message

        Ok(())
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
            ProcessorMiddleMessage::NewTrade(new_trade.id())  // new trade has id.
        )?;

        // TODO: HERE WE HAVE TO HANDLE ProcessingStat

	Ok(())
    }
}
