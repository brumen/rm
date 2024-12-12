use ractor::{Actor, ActorProcessingErr, ActorRef};

use crate::market::{CurrNewMarket, MarketType, TradeMarketDiscovery};
use crate::portfolio::{PortfolioType, PricingResults};
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::{ObtainMarket, ProcessTradeValue};
use crate::trade::{BaseTrade, TradeDirection, TradeReduce, TradeRep};
use crate::processor_curr::{ProcessorCurr, ProcessorCurrMessage};
use crate::ao_trade::AOTrade;


pub struct ProcessorNew<'a>{
    metric: PricingMetric,
    pricing_options: &'a MarketPricingOptions,
    processor_curr: ActorRef<ProcessorCurr<'a>>,
}

#[derive(Debug, Clone)]
pub enum ProcessorNewMessage {
    NewTrade(AOTrade),
    NewMarket(MarketType),
    Behind(i32),
}

pub enum ProcessorNewState {
    Calculating(MarketType),
    Idle(MarketType),
}


impl<'a, ReductionType> RestPricerSpark<ReductionType> for ProcessorNew<'a>
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorNew<'a>: Decoder,
{

    fn _pricing_server_spark(&self) -> String {
	// TODO: CHECK IF CLONING IS GOOD
	self.pricing_options.pricing_server.clone()
    }

    fn _pricing_endpoint_spark(&self, market_: CurrNewMarket, metric: PricingMetric) -> String {
	format!("{}/{:?}/{}", self.pricing_options.pricing_endpoint, market_, metric)
    }
}


impl Actor for ProcessorNew<'_>
//where ReductionType: PartialEq + Clone + BaseTrade + Send + Sync,
{
    type Msg = ProcessorNewMessage;
    // first argument is list of trades, second is the
    //   computation state.
    type State = (TradeRep<AOTrade>, PortfolioType, ProcessorNewState);
    type Arguments = ();  // initialization args.

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
	let (trade_l, portf, pns) = state;

	match message {
	    ProcessorNewMessage::NewTrade(new_trade) => {
		*trade_l += new_trade;  // we add the trade to the list.

		match pns {  // what is the processor doing right now.
		    ProcessorNewState::Calculating(market) => {
			// add the trade to the new portfolio and
			//   attempt again.
			let new_trade_price = new_trade.price(market);
			portf += new_trade_price;  // portfolio update
			self.processor_curr.cast(
			    ProcessorCurrMessage::NewTradePortfolio((*trade_l, *portf))
			);
		    },
		    ProcessorNewState::Idle(market) => {
			// start the new portfolio construction.
			let new_portfolio = self.price_trades_on_spark(
			    &trade_l, self.metric, self.pricing_options, CurrNewMarket::New
			).await;
			portf = &mut new_portfolio;
			pns = &mut ProcessorNewState::Calculating(*market);
			self.processor_curr.cast((trade_l, portf));
		    }
		}
	    },
	    ProcessorNewMessage::NewMarket(new_market) => {
		match pns { // what is the processor doing right now

		    ProcessorNewState::Idle(market) => {
			// we are idle, we can start calculating, start calculating
			let new_portfolio = self.price_trades_on_spark(
			    &trade_l, self.metric, self.pricing_options, CurrNewMarket::New,
			).await;
			// update the state portfolio, calculating, no new trades.
			portf = new_portfolio;
			pns = &mut ProcessorNewState::Calculating(*market);
			// send the message to the current processor
			self.processor_curr.cast((*trade_l, *portf));
		    },

		    ProcessorNewState::Calculating(market) => {
			// ignore if new market comes in, no
			//   action taken.
			// TODO: THIS HAS TO BE REWORKED.
		    },
		}
	    },

	    ProcessorNewMessage::Behind(behind) => {
		// we are behind trades behind the current processor
		match pns { // what is the processor doing right now
		    ProcessorNewState::Idle(market) => {
			// dont do anything.
		    },

		    ProcessorNewState::Calculating(market) => {
			if behind < 0 {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    portf = &mut PortfolioType::default();
			    pns = &mut ProcessorNewState::Idle(*market);
			}  // otherwise dont do anything.
		    },
		}

	    }
	}
	Ok(())
    }
}
