//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use rdkafka::consumer::StreamConsumer;
use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef};

use crate::pricer::PricingMetric;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::MarketPricingOptions;
use crate::ref_deref::TryFromRef;
use crate::trade::TradeRep;
use crate::ao_trade::AOTrade;

// new and current processors.
use crate::processor_curr::ProcessorCurrMessage;
use crate::processor_new::ProcessorNewMessage;

/// ProcessorNew is actor representation of the
///    new processor.
pub struct TradeProducer{
    metric: PricingMetric,
    pricing_options: MarketPricingOptions,
    position_listener: StreamConsumer,
    processor_curr: ActorRef<ProcessorCurrMessage>,
    processor_new: ActorRef<ProcessorNewMessage>,
}


impl TradeProducer {
    pub fn new(
	metric: PricingMetric,
	kafka_server: String,
	kafka_port: String,
	pos_topic: String,
	pricing_options: MarketPricingOptions,
	processor_curr: ActorRef<ProcessorCurrMessage>,
	processor_new: ActorRef<ProcessorNewMessage>,
    ) -> Self {

        let bootstrap_servers = format!("{}:{}", kafka_server, kafka_port);
        let position_listener = connect_with_retries_rd(&bootstrap_servers, &pos_topic);

	Self {
	    metric,
	    pricing_options,
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

	let trade_msg = self.position_listener.recv().await?;
	let trade_1 = AOTrade::try_from_ref(&trade_msg)?;
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

	cast!(
	    self.processor_curr,
	    ProcessorCurrMessage::NewTrade(tr.clone())
	)?;
	cast!(
	    self.processor_new,
	    ProcessorNewMessage::NewTrade(tr.clone())
	)?;
	
        *state += &tr;

	let new_msg = self.position_listener.recv().await?;
	let new_trade = AOTrade::try_from_ref(&new_msg)?;
	myself.send_message(new_trade)?;

	Ok(())
    }
}
