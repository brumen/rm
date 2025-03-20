/// Processor which gets a bulk of work, and finishes it.
use tracing::{info, debug, error, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use futures::future::join_all;

use crate::market::{CurrNewMarket, MarketGeneral};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;  // for trade.value_by_metric2
use crate::trade::{BaseTrade, TradeRep};
//use crate::ao_trade::AOTrade;
use crate::processor_msg::{ProcessorMiddleMessage, ProcessorBulkMessage};


#[derive(Debug)]
pub struct ProcessorBulk<T>{
    pub processor_name: String,
    pub market_name: CurrNewMarket,
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub(crate) trades: TradeRep<T>,
}

#[derive(Debug)]
pub enum ProcessorBulkState {
    Calculating(CurrNewMarket),  // which market we are computing this on.
    Idle,
}


impl<T> Decoder for ProcessorBulk<T> {}

impl<ReductionType, T> RestPricerSpark<ReductionType> for ProcessorBulk<T>
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorBulk<T>: Decoder,
    T: Send + Sync
{

    fn _pricing_server_spark(&self) -> String {
	self.pricing_options.pricing_server.clone()
    }

    fn _pricing_endpoint_spark(
	&self,
	_market_: CurrNewMarket,
	_metric: PricingMetric
    ) -> String {
	"spark".to_string()
    }
}

#[async_trait]
impl<T> Actor for ProcessorBulk<T>
where
    T: Send + Clone + 'static + BaseTrade + ProcessTradeValue + std::fmt::Debug
{
    type Msg = ProcessorBulkMessage<T>;
    type State = i32;  // The number of attempts to run the bulk on, default = 5
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing Bulk processor: {}", self.processor_name);
	Ok(0)  // intialized to 0 attempts.
    }

    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	_state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	match message {
	    ProcessorBulkMessage::NewBulk((market, new_trades, processor_new)) => {
		// start the long-running pricing procedure
		info!(
		    "BulkProcessor {}: NewBulk - Computing {} trades.",
		    self.processor_name,
		    new_trades.len(),
		);

		let mut portfolio = PortfolioType::default();
		let mut pricing_futs = vec![];
		for (_, trade) in new_trades.iter() {
		    info!(
			"Processor {} valuing single trade: {:?}",
			self.processor_name,
			trade
		    );
		    pricing_futs.push(
			trade.value_by_metric2(
			    self.metric, &self.pricing_options,
			    MarketGeneral::MarketRemote(self.market_name.clone())
			)
		    );
		}

		// updating the portfolio
		let pricing_res = join_all(pricing_futs).await;
		for pricing in pricing_res.iter() {
		    portfolio += pricing.aggregate();
		}

		debug!("Bulk processor to middle actor: {:?}", portfolio);
		processor_new.send_message(
		    ProcessorMiddleMessage::BulkReceive(
			(new_trades, portfolio, TradeRep::<T>::default(), market)
		    )
		)?;

	    },
	    ProcessorBulkMessage::Abandon => {
		// stop the computation and go into idle.
	    },
	}

	Ok(())
    }
}
