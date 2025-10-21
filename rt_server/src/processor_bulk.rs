/// Processor which gets a bulk of work, and finishes it.
///
use tracing::{info, debug, error, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use futures::future::join_all;
use std::sync::Arc;

use crate::market::{MarketTypeT};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_msg::{ProcessorMiddleMessage, ProcessorBulkMessage};


#[derive(Debug)]
pub struct ProcessorBulk<T, MP>{
    pub processor_name: String,
    pub metric: PricingMetric,
    pub pricing_options: MP, // MarketPricingOptions,
    // we compute the risk/valuation of the trades in trades
    pub(crate) trade_names: Vec<String>,
    // all_trades is a reference to the structure that contains all trades.
    pub(crate) all_trades: Arc<TradeRep<T>>,
}

#[derive(Debug)]
pub enum ProcessorBulkState<MT> {
    Calculating(MT),  // which market we are computing this on.
    Idle,
}


impl<T, MP> Decoder for ProcessorBulk<T, MP> {}

impl<ReductionType, T, MP> RestPricerSpark<ReductionType> for ProcessorBulk<T, MP>
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorBulk<T, MP>: Decoder,
    T: Send + Sync,
    MP: Send + Sync,
{

    fn _pricing_server_spark(&self) -> String {
	self.pricing_options.pricing_server.clone()
    }

    fn _pricing_endpoint_spark(
	&self,
	_market_: dyn MarketTypeT<MP=MP>,
	_metric: PricingMetric
    ) -> String {
	"spark".to_string()
    }
}


#[async_trait]
impl<T, MP> Actor for ProcessorBulk<T, MP>
where
    T: Send + Sync + Clone + 'static + BaseTrade + std::fmt::Debug + std::fmt::Display,
    MP: Send + Sync + 'static,
    dyn MarketTypeT<MP=MP> + 'static: Sized + Send + Sync,
{
    type Msg = ProcessorBulkMessage<dyn MarketTypeT<MP=MP>>;
    //type State = (i32, dyn MarketTypeT<MP=MP>);  // The number of attempts to run the bulk on, default = 5
    type State = dyn MarketTypeT<MP=MP>;  // The number of attempts to run the bulk on, default = 5
    type Arguments = MP;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing Bulk processor: {}", self.processor_name);
        Ok(
            (0, Self::State::new(self.processor_name.clone(), args)) // TODO: THIS IS WRONG - type of State is not (0, ...)
        )  // intialized to 0 attempts.
    }

    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	match message {
            // market is where the trades are priced.
            // new_trades are trades that should be priced.
            // processor_new ... processor where the result should be sent.
            ProcessorBulkMessage::NewBulk((market, new_trades, processor_new)) => {
		// start the long-running pricing procedure
                let (_, curr_mkt) = state;
                info!(
		    "BulkProcessor {}: NewBulk - Computing {} trades.",
		    self.processor_name,
		    new_trades.len(),
		);
                curr_mkt.market = market.market.clone();

		let mut portfolio = PortfolioType::default();
		let mut pricing_futs = vec![];
		for trade_name in new_trades.iter() {
		    debug!(
			"Processor: {}: valuing single trade: {}",
			self.processor_name,
			trade_name
		    );

                    let trade = self.all_trades.get(trade_name).unwrap();
                    // pricing_futs are futures where the
		    pricing_futs.push(
			trade.value_by_metric2(
			    self.metric,
                            &self.pricing_options,
                            &market,
			)
		    );
		}

		// updating the portfolio
		let pricing_res = join_all(pricing_futs).await;
		for pricing in pricing_res.iter() {
		    portfolio += pricing.aggregate();
		}

		debug!(
                    "Bulk processor {}: portfolio back to middle actor: {:?}",
                    self.processor_name, portfolio,
                );

                // TODO: TRADES THAT DONT PRICE, INCLUDE IN THIS ::default()
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
