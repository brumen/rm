/// Processor which gets a bulk of work, and finishes it.
///
use tracing::{info, debug, warn};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;

use crate::market::{MarketTypeT};
use crate::all_markets::AllMarkets;
use crate::portfolio::PortfolioType;
use crate::pricer::{PricingMetric, PriceTrade};
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_msg::{ProcessorMiddleMessage, ProcessorBulkMessage};


// computes bulk evaluation of trades in trade_names
pub struct ProcessorBulk<T, MT>
where
    MT: MarketTypeT,
{
    pub processor_name: String,  // name of the bulk processor, usually curr_bulk, new_bulk, middle_1_bulk
    pub metric: PricingMetric,
    pub(crate) trade_names: Vec<String>,
    pub(crate) all_trades: Arc<TradeRep<T>>,  // all_trades is a reference to the structure that contains all trades.
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>, // dyn MarketTypeT<MP=MP> + Send + Sync>>>,
}


impl<T, MT> ProcessorBulk<T,MT>
where
    MT: MarketTypeT,
    MT::MP : Clone,
{
    pub(crate) fn new(
        processor_name: String,  // original processor on which this depends.
        metric: PricingMetric,
        all_trades: Arc<TradeRep<T>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
    ) -> Self {

        let bulk_name = format!("{}_bulk", processor_name.clone());

        Self {
            processor_name: bulk_name,
            metric,
            trade_names: vec![],
            all_trades,
            all_markets,
        }
    }
}



pub enum ProcessorBulkState<MT> {
    Calculating(MT),  // which market we are computing this on.
    Idle,
}


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
impl<T, MT> Actor for ProcessorBulk<T, MT>
where
    T: Sync + Send + Clone + BaseTrade + PriceTrade<MT> + 'static,
    MT: MarketTypeT + Send + Sync + 'static,
    MT::MP : Send + Sync + Clone,
{
    type Msg = ProcessorBulkMessage<String>;
    // type State = (usize, Option<dyn MarketTypeT<MP=MP>>);  // The number of attempts to run the bulk on, default = 5
    type State = ();  // which market are we pointing to.
    type Arguments = MT::MP;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing Bulk processor: {}", self.processor_name);
        Ok(())  //  (0, None)  // intialized to 0 attempts.
    }

    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	_state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	match message {
            // market is where the trades are priced.
            // new_trades are trades that should be priced.
            // sending_processor ... processor where the result should be sent.
            ProcessorBulkMessage::NewBulk((market, new_trades, sending_processor)) => {
		// start the long-running pricing procedure

                info!(
		    "{}: NewBulk - Computing {} trades.",
		    self.processor_name,
		    new_trades.len(),
		);

                let Some(market_actual) = self.all_markets.get(&market) else {
                    warn!("Could not get market {}. Abandoning pricing.", market);
                    // if curr_mkt == None, we couldnt get the market, abandon the attempts
                    sending_processor.send_message(
                        ProcessorMiddleMessage::BulkReceive(
                            (new_trades.clone(), PortfolioType::default(), vec![], market.clone())
                        )
                    )?;
                    return Ok(());
                };


                // we have a market
		let mut portfolio = PortfolioType::default();
                let mut non_pricing_trades = Vec::<String>::new();
                let mut used_trades = vec![];
                for trade_name in new_trades.iter() {
                    let Some(trade_attempt) = self.all_trades.get(trade_name) else {
                        warn!("Could not get trade {} from all_trades. Continuing w/o it.", trade_name);
                        non_pricing_trades.push(trade_name.to_string());
                        continue;
                    };
                    used_trades.push(trade_attempt);
                }

                // pricing_futs are futures where the trades are getting priced.
                //let mut pricing_futs = vec![];
                //for used_trade in used_trades {
                for used_trade in new_trades.iter() {
                    let used_trade = self.all_trades.get(used_trade).unwrap();
                    let price = used_trade.value_by_metric(
			self.metric,
                        market_actual.clone(),
		    ).await;
                    debug!("Priced trade {}: {:?}", used_trade.key(), price);
                    portfolio += price.aggregate()
		}
                // TODO: Finish this part here!
		// updating the portfolio
		// let pricing_res = join_all(pricing_futs).await;
                //for pricing in pricing_res.iter() {
		//    portfolio += pricing.aggregate();
		// }

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
