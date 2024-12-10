//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use tracing::{warn, debug};
use rdkafka::consumer::StreamConsumer;
use ractor::{cast, async_trait, Actor, ActorRef, ActorProcessingErr};

use crate::pricer::PricingMetric;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::MarketPricingOptions;
use crate::trade::TradeRep;

// new and current processors.
use crate::processor_curr::{ProcessorCurr, ProcessorCurrMessage};
use crate::processor_new::{ProcessorNew, ProcessorNewMessage};

/// ProcessorNew is actor representation of the
///    new processor.
pub struct TradeProducer<'a, ReductionType>{
    metric: PricingMetric,
    pricing_options: &'a MarketPricingOptions,
    all_trades: TradeRep<ReductionType>,
    existing_trades: TradeRep<ReductionType>,
    position_listener: StreamConsumer,
    processor_curr: ProcessorCurr,
    processor_new: ProcessorNew<'a, ReductionType>,
}


#[async_trait]
impl<'a, ReductionType> Actor for TradeProducer<'a, ReductionType>
where ReductionType: Send + Sync
{
    type Msg = Trade;
    type State = ();
    type Arguments = (String, String, String, ProcessorCurr, ProcessorNew);  // initialization args.

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	// creating the connection server.
	let (kafka_server, kafka_port, pos_topic, processor_curr, processor_new) = args;
	self.processor_new = processor_new;
	self.processor_curr = processor_curr;
        let bootstrap_servers = format!("{}:{}", kafka_server, kafka_port);
        self.position_listener = connect_with_retries_rd(&bootstrap_servers, &pos_topic);

        self.existing_trades = TradeRep::<ReductionType>::default();

	let trade_1 = self.position_listener.recv();
        cast!(myself, trade_1)?;  // first message

        Ok(())
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {
	let new_trade = message;
        match TradeReduce::TradeType::try_from_ref(&message) {
            Err(e) => {
                warn!("Problem w/ trade: {:?}. Ignoring this trade!", e);
            }
            Ok(trade) => {
                debug!("Sending trade {:?} to CURR & NEW processor.", &trade);
		
                // add trades to trade_reduce
                let tr = self.reduce(&trade);
		cast!(
		    self.processor_curr,
		    ProcessorCurrMessage(NewTrade(tr.clone()))
		)?;
                cast!(
		    self.processor_new,
		    ProcessorNewMessage(NewTrade(tr.clone()))
		)?;

                *self.existing_trades.lock().unwrap() += &tr;
            }
        }
	let new_trade = self.position_listener.recv();
	cast!(myself, new_trade)?;  // new position attempts to be handled.

	Ok(())
    }
}
