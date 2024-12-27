use tracing::{info, debug, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::market::{CurrNewMarket, MarketType};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;
use crate::processor_bulk::ProcessorBulkMessage;
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_curr::ProcessorCurrMessage;
use crate::ao_trade::AOTrade;

use crate::market::MarketGeneral;


#[derive(Debug)]
pub struct ProcessorNew{
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub processor_curr: ActorRef<ProcessorCurrMessage>,
    pub processor_bulk: ActorRef<ProcessorBulkMessage>,
}

#[derive(Debug, Clone)]
pub enum ProcessorNewMessage {
    NewTrade(AOTrade),
    NewMarket(MarketType),
    Behind(TradeRep<AOTrade>),  // message from ProcessorCurr, missing trades to calculate.
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    BulkReceive((TradeRep<AOTrade>, PortfolioType, TradeRep<AOTrade>)),  // message from Bulk computation
}

#[derive(Debug)]
pub enum ProcessorNewState {
    CalculatingSingle(MarketType),  // when bulk has finished and we're only calculating single trades.
    CalculatingBulk(MarketType),  // when we're still calculating bulk
    Idle(MarketType),
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

    fn _pricing_endpoint_spark(&self, market_: CurrNewMarket, metric: PricingMetric) -> String {
	format!("{}/{:?}/{}", self.pricing_options.pricing_endpoint, market_, metric)
    }
}

#[async_trait]
impl Actor for ProcessorNew {
    type Msg = ProcessorNewMessage;
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

	let no_market = MarketType::new();
	let empty_portfolio = PortfolioType::default();
        Ok((
	    TradeRep::<AOTrade>::default(),
	    TradeRep::<AOTrade>::default(),
	    empty_portfolio,
	    ProcessorNewState::Idle(no_market))
	)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	//debug!("NEW Processor MSG: {:?}", message);
	
	// match on what message did we get and what state are we in
	let (trade_l, trades_non_pricing, portf, pns) = state;

	//debug!("NEW Processor STATE: {:?}", pns);
	//debug!("NEW Processor TRADES: {:?}", trade_l);
	
	match message {
	    ProcessorNewMessage::NewTrade(new_trade) => {
		*trade_l += &new_trade;  // we add the trade to the list.

		match pns {  // what is the processor doing right now.
		    ProcessorNewState::CalculatingSingle(_market) => {
			// add the trade to the new portfolio and
			//   attempt again.
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric, &self.pricing_options, MarketGeneral::MarketRemote(CurrNewMarket::New)
			).await;
			*portf += new_trade_price;  // portfolio update
			
			// we send the computed portfolio & trades to the current processor
			//   hoping that we are ahead.
			self.processor_curr.send_message(
			    ProcessorCurrMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)
			    )
			)?;
		    },
		    ProcessorNewState::Idle(market) => {
			// start the new portfolio construction.
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk(
				(market.clone(), trade_l.clone(), myself)
			    )
			)?;

			*pns = ProcessorNewState::CalculatingBulk(market.clone());
			*trade_l += &new_trade;
		    },
		    _ => {},
		}
	    },
	    ProcessorNewMessage::NewMarket(_new_market) => {
		match pns { // what is the processor doing right now

		    ProcessorNewState::Idle(_market) => {
			// we are idle, we can start calculating, start calculating
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk((_new_market.clone(), trade_l.clone(), myself))
			)?;
			*pns = ProcessorNewState::CalculatingBulk(_new_market.clone());
			// TODO: MAYBE SOMETHING ELSE
		    },

		    ProcessorNewState::CalculatingSingle(_market) => {
			// attempt to send it to processor current
			self.processor_curr.send_message(
			    ProcessorCurrMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)
			    )
			)?;
		    }
		    // ignore if new market comes in, no
		    //   action taken.
		    _ => {},
		}
	    },

	    // this only comes from ProcessorBulk, so we already launched a bulk request.
	    ProcessorNewMessage::Behind(trades_behind) => {
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
			    *pns = ProcessorNewState::Idle(market.clone());
			} else {
			    // we are still behind the current processor.
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (market.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk(market.clone());
			    *trade_l += &trades_behind;
			}
		    },

		    ProcessorNewState::CalculatingBulk(market) => {
			if trades_behind.is_empty() {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    *pns = ProcessorNewState::Idle(market.clone());			    
			} else {
			    // new processor is behind, calculate the remaining trades.
			    // we are still behind the current processor.
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (market.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk(market.clone());
			    *trade_l += &trades_behind;
			}
		    },

		    ProcessorNewState::Idle(market) => {
			if !trades_behind.is_empty() {
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (market.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk(market.clone());
			    *trade_l += &trades_behind;
			}
		    },
		}
	    },

	    ProcessorNewMessage::BulkReceive((new_trade_l, computed_portf, offending_trades)) => {
		match pns {
		    
		    ProcessorNewState::Idle(_market) => {
		    // 	*portf = computed_portf;
		    // 	*trade_l += &new_trade_l;
		    // 	self.processor_curr.send_message(
		    // 	    ProcessorCurrMessage::NewTradePortfolio(
		    // 		(trade_l.clone(), portf.clone(), _market.clone(), myself)
		    // 	    )
		    // 	)?;
		    // 	*pns = ProcessorNewState::CalculatingSingle(_market.clone());
			
			// TODO: ThE ABOVE SHOULDNT HAPPEN,
			panic!("Received bulk while state = Idle");
		    },

		    ProcessorNewState::CalculatingBulk(_market) => {
			// result of computation has arrived.
			// TODO: FINISH THIS HERE!!!

			debug!("NEW: SENDING MESSAGE TO CURR");
			
			*portf += &computed_portf;
			*trade_l += &new_trade_l;
			*trades_non_pricing += &offending_trades;
			self.processor_curr.send_message(
			    ProcessorCurrMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), _market.clone(), myself)
			    )
			)?;
			*pns = ProcessorNewState::CalculatingSingle(_market.clone());
		    },
		    ProcessorNewState::CalculatingSingle(_market) => {
			// result of computation has arrived.
			// TODO: FINISH THIS HERE!!!
			panic!("Received bulk while calculating single - Weird");
		    },		    
		}
	    }
	}
	Ok(())
    }
}
