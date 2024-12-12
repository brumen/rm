//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use tracing::{warn, debug};
use rdkafka::consumer::StreamConsumer;
use ractor::{cast, Actor, ActorRef, ActorProcessingErr};

use crate::pricer::PricingMetric;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::MarketPricingOptions;
use crate::trade::TradeRep;
use crate::ao_trade::AOTrade;

// new and current processors.
use crate::processor_curr::{ProcessorCurr, ProcessorCurrMessage};
use crate::processor_new::{ProcessorNew, ProcessorNewMessage};

/// ProcessorNew is actor representation of the
///    new processor.
pub struct TradeProducer<'a, ReductionType>{
    metric: PricingMetric,
    pricing_options: &'a MarketPricingOptions,
    position_listener: StreamConsumer,
    processor_curr: ActorRef<ProcessorCurr<'a, ReductionType>>,
    processor_new: ActorRef<ProcessorNew<'a, ReductionType>>,
}


impl<'a, ReductionType> TradeProducer<'a, ReductionType> {
    fn new(
	metric: PricingMetric,
	kafka_server: String,
	kafka_port: String,
	pos_topic: String,
	pricing_options: &'a MarketPricingOptions,
	processor_curr: ActorRef<ProcessorNew<'a, ReductionType>>,
	processor_new: ActorRef<ProcessorNew<'a, ReductionType>>,
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


impl<'a, ReductionType, TradeType> Actor for TradeProducer<'a, ReductionType>
where ReductionType: Send + Sync
{
    type Msg = TradeType;
    type State = TradeRep<ReductionType>;  // list of existing trades.
    // (kafka server, kafka port, position topic)
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let trade_1 = self.position_listener.recv();
        myself.send_message(trade_1)?;  // first message

        Ok(TradeRep::default())  // default empty state.
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {

        match TradeReduce::TradeType::try_from_ref(&message) {
            Err(e) => {
                warn!("Problem w/ trade: {:?}. Ignoring this trade!", e);
            }
            Ok(trade) => {
                debug!("Sending trade {:?} to CURR & NEW processor.", &trade);
		
                // add trades to trade_reduce
                let tr = trade.reduce();

		self.processor_curr.send_message(ProcessorCurrMessage::NewTrade(tr.clone()))?;
		self.processor_new.send_message(ProcessorNewMessage::NewTrade(tr.clone()))?;

                *state += &tr;
            }
        }

	let new_trade = self.position_listener.recv().await;  // gives control to others.
	myself.send_message(&new_trade)?;

	Ok(())
    }
}
