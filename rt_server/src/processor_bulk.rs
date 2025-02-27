/// Processor which gets a bulk of work, and finishes it.
use tracing::{info, debug, error, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use futures::future::join_all;

use crate::market::{CurrNewMarket, MarketGeneral, MarketType};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;  // for trade.value_by_metric2
use crate::trade::{BaseTrade, TradeRep};
use crate::ao_trade::AOTrade;
use crate::processor_msg::{ProcessorMiddleMessage, ProcessorBulkMessage};


#[derive(Debug)]
pub struct ProcessorBulk{
    pub processor_name: String,
    pub market_name: CurrNewMarket,
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
}

#[derive(Debug)]
pub enum ProcessorBulkState {
    Calculating(MarketType),
    Idle,
}


impl Decoder for ProcessorBulk {}

impl<ReductionType> RestPricerSpark<ReductionType> for ProcessorBulk
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorBulk: Decoder,
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
impl Actor for ProcessorBulk {
    type Msg = ProcessorBulkMessage;
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
		let nb_trades = new_trades.len();
		debug!(
		    "Bulk processor {} processor. Computing {} trades.",
		    self.processor_name,
		    nb_trades,
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
			(new_trades, portfolio, TradeRep::<AOTrade>::default(), market)
		    )
		)?;

	    },
	    ProcessorBulkMessage::Abandon => {
		// stop the computation and go into idle.
	    },
	}

	Ok(())
    }

    
    // async fn handle(
    //     &self,
    // 	myself: ActorRef<Self::Msg>,
    // 	message: Self::Msg,
    // 	state: &mut Self::State,
    // ) -> Result<(), ActorProcessingErr> {

    // 	let ProcessorBulkMessage::NewBulk((market, new_trades, processor_new)) = message; 
    // 	// start the long-running pricing procedure
    // 	debug!("BULK Processor TRADES: {:?}", new_trades);
    // 	let new_portfolio = self.price_trades_on_spark(
    // 	    &new_trades, self.metric, &self.pricing_options, CurrNewMarket::New,
    // 	).await;

    // 	match new_portfolio {
    // 	    Ok(np) => {
    // 		debug!("BULK Processor TO NEW: {:?}", np);
    // 		processor_new.send_message(
    // 		    ProcessorNewMessage::BulkReceive(
    // 			(new_trades, np, TradeRep::<AOTrade>::default(), market)
    // 		    )
    // 		)?;
    // 	    },
    // 	    Err(e) => {
    // 		error!("BULK processor ERROR: {}", e);
    // 		error!("Retrying the bulk calculation");
    // 		if *state < 5 {  // TODO: THIS 5 SHOULDNT BE HARDCODED HERE!!!
    // 		    *state += 1;
    // 		    myself.send_message(
    // 			ProcessorBulkMessage::NewBulk(
    // 			    (market, new_trades, processor_new)
    // 			)
    // 		    )?;
    // 		} else {

    // 		    // compute trades one by one
    // 		    let mut offending_trades = TradeRep::<AOTrade>::default();
    // 		    let mut new_portfolio = PortfolioType::default();
    // 		    for (_, trade) in new_trades.iter() {
    // 			let valued_trade = (*trade).value_by_metric2(
    // 			    self.metric, &self.pricing_options,
    // 			    MarketGeneral::MarketRemote(CurrNewMarket::Current)
    // 			).await;
    // 			// TODO: WHERE IS THE FAILURE HERE???
    // 			new_portfolio += valued_trade;
    // 		    }
		    
    // 		    processor_new.send_message(
    // 			ProcessorNewMessage::BulkReceive(
    // 			    (new_trades, new_portfolio, offending_trades, market)
    // 			)
    // 		    )?;
    // 		}		
    // 	    },
    // 	}
    // 	Ok(())
    // }
}
