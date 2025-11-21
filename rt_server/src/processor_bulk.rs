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
    pub processor_name: String,
    pub metric: PricingMetric,
    pub(crate) trade_names: Vec<String>,
    pub(crate) all_trades: Arc<TradeRep<T>>,  // all_trades is a reference to the structure that contains all trades.
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>, // dyn MarketTypeT<MP=MP> + Send + Sync>>>,
}


impl<T, MT> ProcessorBulk<T,MT>
where
    MT: MarketTypeT,
{
    pub(crate) fn new(
        processor_name: String,
        metric: PricingMetric,
        all_trades: Arc<TradeRep<T>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
        mp: MT::MP,
    ) -> Self {

        let bulk_mkt = MT::new(processor_name.clone(), mp);

        // insert a proper market into the all_market.
        // all_markets.insert(
        //     processor_name.clone(),
        //     bulk_mkt,
        // );

        Self {
            processor_name,
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


// impl<T, MP> Decoder for ProcessorBulk<T, MP>
// where
//     dyn MarketTypeT<MP=MP>: Sized + std::fmt::Debug,
//     dyn MarketTypeT<MP=MP> + Send + Sync + 'static: Sized,
// {}



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
// dyn MarketTypeT<MP=MP> + Send + Sync: Sized,
//     dyn MarketTypeT<MP=MP>: Sized + Sync + Send,
//    MP: 'static + Send + Sync + Clone,
    // Arc<dyn MarketTypeT<MP=MP> + Send + Sync>: MarketTypeT<MP=MP> + Clone,
    MT: MarketTypeT + Send + Sync + 'static,
    MT::MP : Send + Sync,
{
    type Msg = ProcessorBulkMessage<String>;  // dyn MarketTypeT<MP=MP>>;
    // type State = (usize, Option<dyn MarketTypeT<MP=MP>>);  // The number of attempts to run the bulk on, default = 5
    type State = Option<Arc<MT>>;  // dyn MarketTypeT<MP=MP>>;
    type Arguments = MT::MP;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	info!("Initializing Bulk processor: {}", self.processor_name);
        Ok(None)  //  (0, None)  // intialized to 0 attempts.
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
		    "BulkProcessor {}: NewBulk - Computing {} trades.",
		    self.processor_name,
		    new_trades.len(),
		);
                let curr_mkt_attempt = self.all_markets.markets.get(&market);

                // if curr_mkt == None, we couldnt get the market, abandon the attempts
                if curr_mkt_attempt.is_none() {
                    sending_processor.send_message(
                        ProcessorMiddleMessage::BulkReceive(
                            (new_trades.clone(), PortfolioType::default(), vec![], market.clone())
                        )
                    )?;
                }

                // we have a market
		let mut portfolio = PortfolioType::default();

                let mut non_pricing_trades = Vec::<String>::new();

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

                let market_actual = self.all_markets.markets.get(&market).unwrap();
                let market_actual = market_actual.value();
                // pricing_futs are futures where the trades are getting priced.
                //let mut pricing_futs = vec![];
                //for used_trade in used_trades {
                for used_trade in new_trades.iter() {
                    let used_trade = self.all_trades.get(used_trade).unwrap();
                    let price = used_trade.value_by_metric(
			self.metric,
                        market_actual.clone(),
		    ).await;
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
