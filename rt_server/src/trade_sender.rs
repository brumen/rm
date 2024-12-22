//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use tracing::info;
use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::portfolio_sender::connect_with_retries_rd;
use crate::ref_deref::TryFromRef;
use crate::trade::TradeRep;
use crate::ao_trade::AOTrade;

// new and current processors.
use crate::processor_curr::ProcessorCurrMessage;
use crate::processor_new::ProcessorNewMessage;

pub struct TradeProducer{
    position_listener: StreamConsumer,
    processor_curr: ActorRef<ProcessorCurrMessage>,
    processor_new: ActorRef<ProcessorNewMessage>,
}


impl TradeProducer {
    pub fn new(
	kafka_server: String,
	pos_topic: String,
	processor_curr: ActorRef<ProcessorCurrMessage>,
	processor_new: ActorRef<ProcessorNewMessage>,
    ) -> Self {

	info!("Starting trade producer on {:?}", pos_topic);
	let position_listener = connect_with_retries_rd(&kafka_server, &pos_topic);

	Self {
	    position_listener,
	    processor_curr,
	    processor_new,
	}
    }
}


#[async_trait]
impl Actor for TradeProducer {
    type Msg = AOTrade;
    type State = TradeRep<AOTrade>;  // list of existing trades.
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initiating TradeProducer");
	let trade_msg = self.position_listener.recv().await?;
	let trade_1 = AOTrade::try_from_ref(&trade_msg)?;
	info!("First trade: {:?}", trade_1);
        myself.send_message(trade_1)?;  // first message

        Ok(TradeRep::default())  // default empty state.
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {

        // add trades to trade_reduce
        let tr = message;

	self.processor_curr.send_message(
	    ProcessorCurrMessage::NewTrade(tr.clone())
	)?;
	self.processor_new.send_message(
	    ProcessorNewMessage::NewTrade(tr.clone())
	)?;
	
        *state += &tr;

	let new_msg = self.position_listener.recv().await?;
	let new_trade = AOTrade::try_from_ref(&new_msg)?;
	myself.send_message(new_trade)?;

	Ok(())
    }
}
