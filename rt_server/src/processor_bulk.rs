/// Processor which gets a bulk of work, and finishes it.
///
use tracing::{info, debug, warn};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use futures::future::join_all;
use std::sync::Arc;

use crate::market::{MarketTypeT};
use crate::all_markets::AllMarkets;
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, PricingMetric, PriceTrade};  // MarketPricingOptions,  RestPricerSpark
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_msg::{ProcessorMiddleMessage, ProcessorBulkMessage};


#[derive(Debug)]
pub struct ProcessorBulk<T, MP>
where
    dyn MarketTypeT<MP=MP>: Sized + std::fmt::Debug
{
    pub processor_name: String,
    pub metric: PricingMetric,
    // pub pricing_options: MP, // MarketPricingOptions,
    // we compute the risk/valuation of the trades in trades
    pub(crate) trade_names: Vec<String>,
    // all_trades is a reference to the structure that contains all trades.
    pub(crate) all_trades: Arc<TradeRep<T>>,
    pub(crate) all_markets: Arc<AllMarkets<dyn MarketTypeT<MP=MP>>>,
}

#[derive(Debug)]
pub enum ProcessorBulkState<MT> {
    Calculating(MT),  // which market we are computing this on.
    Idle,
}


impl<T, MP> Decoder for ProcessorBulk<T, MP>
where
    dyn MarketTypeT<MP=MP>: Sized + std::fmt::Debug
{}

// impl<ReductionType, T, MP> RestPricerSpark<ReductionType> for ProcessorBulk<T, MP>
// where
//     ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
//     ProcessorBulk<T, MP>: Decoder,
//     T: Send + Sync,
//     MP: Send + Sync,
// {

//     fn _pricing_server_spark(&self) -> String {
// 	self.pricing_options.pricing_server.clone()
//     }

//     fn _pricing_endpoint_spark(
// 	&self,
// 	_market_: dyn MarketTypeT<MP=MP>,
// 	_metric: PricingMetric
//     ) -> String {
// 	"spark".to_string()
//     }
// }


#[async_trait]
impl<T, MP> Actor for ProcessorBulk<T, MP>
where
    T: Send + Sync + Clone + 'static + BaseTrade + PriceTrade<MP> + std::fmt::Debug + std::fmt::Display,
    MP: Send + Sync + 'static,
    dyn MarketTypeT<MP=MP> + 'static: Sized + Send + Sync + std::fmt::Debug,
{
    type Msg = ProcessorBulkMessage<dyn MarketTypeT<MP=MP>>;
    // type State = (usize, Option<dyn MarketTypeT<MP=MP>>);  // The number of attempts to run the bulk on, default = 5
    type State = Option<dyn MarketTypeT<MP=MP>>;
    type Arguments = MP;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing Bulk processor: {}", self.processor_name);
        Ok(
            (0, None)  // intialized to 0 attempts.
        )
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
            // sending_processor ... processor where the result should be sent.
            ProcessorBulkMessage::NewBulk((market, new_trades, sending_processor)) => {
		// start the long-running pricing procedure
                let curr_mkt = state;

                info!(
		    "BulkProcessor {}: NewBulk - Computing {} trades.",
		    self.processor_name,
		    new_trades.len(),
		);
                let curr_mkt_attempt = self.all_markets.get(&market);  //

                // if curr_mkt == None, we couldnt get the market, abandon the attempts
                if curr_mkt_attempt.is_none() {
                    sending_processor.send_message(
                        ProcessorMiddleMessage::BulkReceive(
                            (new_trades.clone(), PortfolioType::default(), vec![], market.clone())
                        )
                    )?;
                }

                // we have a market
                let curr_mkt = curr_mkt_attempt.unwrap();
                let market_params = curr_mkt.market_params();  // market params

		let mut portfolio = PortfolioType::default();
		let mut pricing_futs = vec![];
                let mut non_pricing_trades = Vec::<String>::new();

                let market_actual = self.all_markets.get(&market).unwrap();
                let market_actual_val = market_actual.value();

                let mut used_trades = vec![];
                for trade_name in new_trades.iter() {
		    debug!(
			"Processor: {}: valuing single trade: {}",
			self.processor_name,
			trade_name
		    );

                    let trade_attempt = self.all_trades.get(trade_name);
                    if trade_attempt.is_none() {
                        warn!("Could not get trade {} from all_trades. Continuing w/o it.", trade_name);
                        non_pricing_trades.push(trade_name.to_string());
                        continue;
                    }
                    let trade = trade_attempt.unwrap();
                    used_trades.push(trade);
                }
                    // pricing_futs are futures where the trades are getting priced.
                for used_trade in used_trades {
                    pricing_futs.push(
			used_trade.value_by_metric(
			    self.metric,
                            market_actual_val,
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

		sending_processor.send_message(
		    ProcessorMiddleMessage::BulkReceive(
			(new_trades, portfolio, non_pricing_trades, market)
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
