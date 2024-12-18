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
    Behind(i32),  // message from ProcessorCurr
    BulkReceive((TradeRep<AOTrade>, PortfolioType)),  // message from Bulk computation
}

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
    // first argument is list of trades, second is the
    //   current new portfolio, third is the computation state.
    type State = (TradeRep<AOTrade>, PortfolioType, ProcessorNewState);
    type Arguments = ();

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let no_market = MarketType::new();
	let empty_portfolio = PortfolioType::default();
        Ok(
	    (TradeRep::<AOTrade>::default(), empty_portfolio, ProcessorNewState::Idle(no_market))
	)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	// match on what message did we get and what state are we in

	// TODO: THIS IS WRONG HERE!!! MUTABLE REFERENCE PATTERN!!!
	let (trade_l, portf, pns) = state;
	
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
				(trade_l.clone(), portf.clone(), myself)
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

		    ProcessorNewState::Idle(market) => {
			// we are idle, we can start calculating, start calculating
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk((market.clone(), trade_l.clone(), myself))
			)?;
			*pns = ProcessorNewState::CalculatingBulk(market.clone());
			// TODO: MAYBE SOMETHING ELSE
		    },

		    // ignore if new market comes in, no
		    //   action taken.
		    _ => {},
		}
	    },

	    ProcessorNewMessage::Behind(behind) => {
		// we are behind trades behind the current processor
		match pns { // what is the processor doing right now
		    ProcessorNewState::CalculatingSingle(market) => {
			if behind < 0 {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    *pns = ProcessorNewState::Idle(market.clone());  // TODO: CHECK THIS CLONE
			    // TODO: CONSIDER TRADES!!!
			}  // otherwise dont do anything.
		    },

		    ProcessorNewState::CalculatingBulk(market) => {
			if behind < 0 {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    *pns = ProcessorNewState::Idle(market.clone());  // TODO: CHECK THIS 
			    // TODO: CONSIDER TRADES!!!
			    
			}  // otherwise dont do anything.
		    },
		    _ => {},
		}
	    },

	    ProcessorNewMessage::BulkReceive((new_trade_l, computed_portf)) => {
		match pns {
		    ProcessorNewState::Idle(_market) => {
			// TODO: WEIRD THIS SHOULDNT BE HERE
			panic!("Received bulk while state = Idle");
		    },

		    ProcessorNewState::CalculatingBulk(_market) => {
			// result of computation has arrived.
			// TODO: FINISH THIS HERE!!!

			*portf = computed_portf;
			*trade_l = new_trade_l;
			self.processor_curr.send_message(
			    ProcessorCurrMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), myself)
			    )
			)?;
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
