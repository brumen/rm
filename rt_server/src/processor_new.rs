use tracing::{info, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::market::{CurrNewMarket, MarketSwitching, MarketType, MarketGeneral};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage,};
use crate::trade::{BaseTrade, TradeRep};
use crate::ao_trade::AOTrade;


#[derive(Debug)]
pub struct ProcessorNew{
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub processor_curr: ActorRef<ProcessorMiddleMessage>,  // current processor ref.
    pub processor_bulk: ActorRef<ProcessorBulkMessage>,  // bull processor ref.
    pub r_client: Option<reqwest::Client>,
}

#[derive(Debug)]
pub enum ProcessorNewState {
    CalculatingSingle(MarketType),  // when bulk has finished and we're only calculating single trades.
    CalculatingBulk(MarketType),  // when we're still calculating bulk
    Idle(MarketType),
}


impl MarketSwitching for ProcessorNew {
    
    fn r_client(&self) ->  &reqwest::Client {
	match &self.r_client {
	    Some(rc) => return &rc,
	    None => panic!("Need client for market switching"),
	}
    }

    fn market_endpoint(&self) -> String {
	format!("http://{0}/market", self.pricing_options.pricing_server.clone())
    }
}


impl Decoder for ProcessorNew {}

impl<ReductionType> RestPricerSpark<ReductionType> for ProcessorNew
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorNew: Decoder,
{

    fn _pricing_server_spark(&self) -> String {
	// TODO: CHECK IF CLONING IS GOOD
	self.pricing_options.pricing_server.clone()
    }

    fn _pricing_endpoint_spark(&self, _market_: CurrNewMarket, _metric: PricingMetric) -> String {
	"/pricing".to_string()
    }
}


#[async_trait]
impl Actor for ProcessorNew {
    type Msg = ProcessorMiddleMessage;
    // first argument is list of trades,
    //   second is the list of trades that didnt price correctly
    //   third is the current portfolio result of correctly pricing trades.
    //   fourth is the computation state.
    type State = (TradeRep<AOTrade>, TradeRep<AOTrade>, PortfolioType, ProcessorNewState);
    type Arguments = ();

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

        Ok((
	    TradeRep::<AOTrade>::default(),
	    TradeRep::<AOTrade>::default(),
	    PortfolioType::default(),
	    ProcessorNewState::Idle(MarketType::new()))
	)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
	
	let (trade_l, trades_non_pricing, portf, pns) = state;

	let new_m = CurrNewMarket("new".to_string());
	
	match message {
	    ProcessorMiddleMessage::NewTrade(new_trade) => {
		//*trade_l += &new_trade;  // we add the trade to the list.

		match pns {  // what is the processor doing right now.
		    ProcessorNewState::CalculatingSingle(_market) => {
			// add the trade to the new portfolio and
			//   attempt again.
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric,
			    &self.pricing_options,
			    MarketGeneral::MarketRemote(new_m),
			).await;
			*portf += new_trade_price;  // portfolio update
			*trade_l += &new_trade;  // we add the trade to the list.
			
			// we send the computed portfolio & trades to the current processor
			//   hoping that we are ahead.
			self.processor_curr.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)
			    )
			)?;
		    },
		    
		    ProcessorNewState::Idle(market) => {
			// start the new portfolio construction.
			*trade_l += &new_trade;
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk(
				(market.clone(), trade_l.clone(), myself)
			    )
			)?;
			*pns = ProcessorNewState::CalculatingBulk(market.clone());
		    },
		    
		    ProcessorNewState::CalculatingBulk(_market) => {
			*trade_l += &new_trade;  // we add the trade to the list.
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric,
			    &self.pricing_options,
			    MarketGeneral::MarketRemote(new_m)
			).await;
			*portf += new_trade_price;  // portfolio update
		    },
		}
	    },
	    ProcessorMiddleMessage::NewMarket(new_market) => {
		match pns { // what is the processor doing right now

		    ProcessorNewState::Idle(_market) => {
			// we are idle, we can start calculating, start calculating
			*pns = ProcessorNewState::CalculatingBulk(new_market.clone());
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk((new_market.clone(), trade_l.clone(), myself))
			)?;
			// update the market on the server
			
		    },

		    ProcessorNewState::CalculatingSingle(market) => {
			// attempt to send it to processor current
			self.processor_curr.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), market.clone(), myself)
			    )
			)?;
		    }
		    // ignore if new market comes in, no
		    //   action taken.
		    _ => {},
		}
	    },

	    // this only comes from ProcessorBulk, so we already launched a bulk request.
	    ProcessorMiddleMessage::Behind(trades_behind) => {
		// we are behind trades behind the current processor
		match pns { // what is the processor doing right now
		    ProcessorNewState::CalculatingSingle(market) => {
			// TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
			// TRADES ARE BEHIND,
			//    < 10 -> continue in single mode
			//    > 10 -> continue in bulk mode.
			if trades_behind.is_empty() {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    let _ = self.switch_market(market.clone()).await;
			    *pns = ProcessorNewState::Idle(market.clone());

			} else {
			    // we are still behind the current processor.
			    *trade_l += &trades_behind;
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (market.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk(market.clone());
			}
		    },

		    // TODO: THIS HAS TO BE REEXAMINED!!!!
		    ProcessorNewState::CalculatingBulk(market) => {
			if trades_behind.is_empty() {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    *pns = ProcessorNewState::Idle(market.clone());
			}
			// otherwise we wait for the results of bulk computation.
			
			// } else {
			//     // new processor is behind, calculate the remaining trades.
			//     // we are still behind the current processor.
			//     self.processor_bulk.send_message(
			// 	ProcessorBulkMessage::NewBulk(
			// 	    (market.clone(), trades_behind.clone(), myself)
			// 	)
			//     )?;
			//     // *trade_l += &trades_behind;
			// }
		    },

		    ProcessorNewState::Idle(market) => {
			if !trades_behind.is_empty() {
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (market.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk(market.clone());
			    //*trade_l += &trades_behind;
			}
		    },
		}
	    },

	    ProcessorMiddleMessage::BulkReceive((new_trade_l, computed_portf, offending_trades, _bulk_market)) => {
		match pns {
		    
		    ProcessorNewState::Idle(_market) => {
			info!("Ignoring bulk receive.");
		    },

		    ProcessorNewState::CalculatingBulk(market) => {
			// result of computation has arrived.
			// TODO: FINISH THIS HERE!!!			
			*portf += &computed_portf;
			*trade_l += &new_trade_l;
			*trades_non_pricing += &offending_trades;
			self.processor_curr.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), market.clone(), myself)
			    )
			)?;
			*pns = ProcessorNewState::CalculatingSingle(market.clone());
		    },
		    ProcessorNewState::CalculatingSingle(_market) => {
			// result of computation has arrived.
			//  add it to the computation
			// TODO: FINISH THIS HERE!!!
			// panic!("Received BulkReceive while calculating single - Weird");

			*portf = computed_portf;
			*trade_l += &new_trade_l;
			self.processor_curr.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)
			    )
			)?;
		    },		    
		}
	    },

	    _ => {
		// this type shouldnt occur
		//   TODO: Better error message
		panic!("Message type shouldnt occur");
	    },  
	}
	Ok(())
    }
}
