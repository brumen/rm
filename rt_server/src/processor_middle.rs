// middle processor, sits between 2 new processors

use tracing::{info, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::market::{CurrNewMarket, MarketSwitching, MarketType, MarketGeneral};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;
use crate::processor_bulk::ProcessorBulkMessage;
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_curr::ProcessorCurrMessage;
use crate::ao_trade::AOTrade;


#[derive(Debug)]
pub(crate) struct ProcessorMiddle{
    pub(crate) metric: PricingMetric,
    pub(crate) pricing_options: MarketPricingOptions,
    market_name: String,
    pub processor_below: ActorRef<ProcessorMiddleMessage>,  // processor below
    pub processor_above: ActorRef<ProcessorMiddleMessage>,  // processor above
    pub processor_bulk: ActorRef<ProcessorBulkMessage>,  // bull processor ref.
    pub r_client: Option<reqwest::Client>,
}

/// message that the new processor receives
#[derive(Debug, Clone)]
pub enum ProcessorMiddleMessage {
    NewTrade(AOTrade),
    NewMarket(MarketType),
    Behind(TradeRep<AOTrade>),  // message from Processor_below, missing trades to calculate.
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    // fourth: market on which these trades were computed.
    BulkReceive((TradeRep<AOTrade>, PortfolioType, TradeRep<AOTrade>, MarketType)),  // message from Bulk computation
    // message from the processor above.
    NewTradePortfolio(
	(TradeRep<AOTrade>, PortfolioType, MarketType, ActorRef<ProcessorMiddleMessage>)
    ),

}

#[derive(Debug)]
pub enum ProcessorMiddleState {
    CalculatingSingle(MarketType),  // when bulk has finished and we're only calculating single trades.
    CalculatingBulk(MarketType),  // when we're still calculating bulk
    Idle(MarketType),
}


impl MarketSwitching for ProcessorMiddle {
    fn r_client(&self) ->  &reqwest::Client {
	match &self.r_client {
	    Some(rc) => return &rc,
	    None => panic!("Need client for market switching"),
	}
    }

    fn market_endpoint(&self) -> String {
	// TODO: THIS NEEDS TO BE FIXED.
	format!("http://{0}/market", self.pricing_options.pricing_server.clone())
    }
}

impl Decoder for ProcessorMiddle {}


// TODO: IMPLEMENT RestPricerSpark HERE MISSING


#[async_trait]
impl Actor for ProcessorMiddle {
    type Msg = ProcessorMiddleMessage;
    // first argument is list of trades,
    //   second is the list of trades that didnt price correctly
    //   third is the current portfolio result of correctly pricing trades.
    //   fourth is the computation state.
    type State = (TradeRep<AOTrade>, TradeRep<AOTrade>, PortfolioType, ProcessorMiddleState);
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
	    ProcessorMiddleState::Idle(MarketType::new()))
	)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
	
	let (trade_l, trades_non_pricing, portf, pns) = state;
	
	match message {
	    ProcessorMiddleMessage::NewTrade(new_trade) => {
		match pns {  // what is the processor doing right now.
		    ProcessorMiddleState::CalculatingSingle(_market) => {
			// add the trade to the new portfolio and
			//   attempt again.
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric, &self.pricing_options, MarketGeneral::MarketRemote(CurrNewMarket::New)
			).await;
			*portf += new_trade_price;  // portfolio update
			*trade_l += &new_trade;  // we add the trade to the list.
			
			// we send the computed portfolio & trades to the current processor
			//   hoping that we are ahead.
			self.processor_below.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)
			    )
			)?;
		    },
		    
		    ProcessorMiddleState::Idle(market) => {
			// start the new portfolio construction.
			*trade_l += &new_trade;
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk(
				(market.clone(), trade_l.clone(), myself)  // TODO: THIS HAS TO BE REWORKED????
			    )
			)?;
			*pns = ProcessorMiddleState::CalculatingBulk(market.clone());
		    },
		    
		    ProcessorMiddleState::CalculatingBulk(_market) => {
			*trade_l += &new_trade;  // we add the trade to the list.
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric, &self.pricing_options, MarketGeneral::MarketRemote(CurrNewMarket::New)  // TODO: MARKET HERE IS WRONG
			).await;
			*portf += new_trade_price;  // portfolio update
		    },
		}
	    },

	    ProcessorMiddleMessage::NewMarket(new_market) => {
		match pns { // what is the processor doing right now

		    ProcessorMiddleState::Idle(_market) => {
			// we are idle, we can start calculating, start calculating
			*pns = ProcessorMiddleState::CalculatingBulk(new_market.clone());
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk((new_market.clone(), trade_l.clone(), myself))  // TODO: MYSELF HERE IS WRONG!!!
			)?;
		    },

		    ProcessorMiddleState::CalculatingSingle(market) => {
			// attempt to send it to processor current
			self.processor_below.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), market.clone(), myself)  // TODO: CHECK myself here
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
		    ProcessorMiddleState::CalculatingSingle(market) => {
			// TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
			// TRADES ARE BEHIND,
			//    < 10 -> continue in single mode
			//    > 10 -> continue in bulk mode.
			if trades_behind.is_empty() {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    let _ = self.switch_market(
				market.clone(), CurrNewMarket::New
			    ).await;
			    *pns = ProcessorMiddleState::Idle(market.clone());

			} else {
			    // we are still behind the current processor.
			    *trade_l += &trades_behind;
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (market.clone(), trades_behind.clone(), myself)  // TODO: MYSELF IS WRONG HERE
				)
			    )?;
			    *pns = ProcessorMiddleState::CalculatingBulk(market.clone());
			}
		    },

		    // TODO: THIS HAS TO BE REEXAMINED!!!!
		    ProcessorMiddleState::CalculatingBulk(market) => {
			if trades_behind.is_empty() {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    *pns = ProcessorMiddleState::Idle(market.clone());
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

		    ProcessorMiddleState::Idle(market) => {
			if !trades_behind.is_empty() {
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (market.clone(), trades_behind.clone(), myself)  // TODO: MYELF HERE WRONG!!!
				)
			    )?;
			    *pns = ProcessorMiddleState::CalculatingBulk(market.clone());
			    //*trade_l += &trades_behind;
			}
		    },
		}
	    },

	    ProcessorMiddleMessage::BulkReceive((new_trade_l, computed_portf, offending_trades, _bulk_market)) => {
		match pns {
		    
		    ProcessorMiddleState::Idle(_market) => {
			info!("Ignoring bulk receive.");
		    },

		    ProcessorMiddleState::CalculatingBulk(market) => {
			// result of computation has arrived.
			// TODO: FINISH THIS HERE!!!			
			*portf += &computed_portf;
			*trade_l += &new_trade_l;
			*trades_non_pricing += &offending_trades;
			self.processor_below.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), market.clone(), myself)  // TODO: MYSELF IS WRONG
			    )
			)?;
			*pns = ProcessorMiddleState::CalculatingSingle(market.clone());
		    },
		    ProcessorMiddleState::CalculatingSingle(_market) => {
			// result of computation has arrived.
			//  add it to the computation
			// TODO: FINISH THIS HERE!!!
			// panic!("Received BulkReceive while calculating single - Weird");

			*portf = computed_portf;
			*trade_l += &new_trade_l;
			self.processor_below.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)  // TODO: MYSELF IS WRONG
			    )
			)?;
		    },		    
		}
	    },

	    // receiving a mesage from the processor above
	    ProcessorMiddleMessage::NewTradePortfolio(ntp) => {  // ntp = (trades, potential_portfolio, market, upstream_processor)
		match pns {
		    
		    ProcessorMiddleState::Idle(_market) => {
			// just pass it to the processor below, dont do anything else
			self.processor_below.send_message(ntp)?;  // TODO: CHECK THIS!!!
		    },

		    ProcessorMiddleState::CalculatingBulk(market) => {
			// we got new portfolio, but we are in the process of computing the portfolresult of computation has arrived.
			let (trades, potential_portfolio, new_market, _) = ntp;
			
			if new_market.is_later_than(market) {  // TODO: THIS IS NOT SUFFICIENT CONDITION
			    // replace the portfolio, and pass it down

			    // TODO: ALSO, SHOULDNT WE NOTIFY THE BULK PROCESSOR THAT WE ARE ABANDONING THE ATTEMPT.
			    self.processor_below.send_message(
				ProcessorMiddleMessage::NewTradePortfolio(
				    (trades, potential_portfolio, new_market, myself)
				)
			    )?;
			    // set the state of this processor to the state being sent.
			    *portf = potential_portfolio;
			    *trade_l = trades;
			    *state = ProcessorMiddleState::Idle(new_market);
			}
		    },
		    ProcessorMiddleState::CalculatingSingle(_market) => {
			// result of computation has arrived.
			//  add it to the computation
			// TODO: FINISH THIS HERE!!!
			// panic!("Received BulkReceive while calculating single - Weird");

			*portf = computed_portf;
			*trade_l += &new_trade_l;
			self.processor_below.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)  // TODO: MYSELF IS WRONG
			    )
			)?;
		    },		    
		}
		
	    },	    
	}
	Ok(())
    }
}
