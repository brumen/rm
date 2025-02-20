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
	    match pns {  // what is the processor doing right now.
		ProcessorNewState::CalculatingSingle(_market) => {
		    // add the trade to the new portfolio and
		    //   attempt again.
		    let new_trade_price = new_trade.value_by_metric2(
			self.metric, &self.pricing_options, MarketGeneral::MarketRemote(CurrNewMarket::New)
		    ).await;
		    *portf += new_trade_price;  // portfolio update
		    *trade_l += &new_trade;  // we add the trade to the list.
		    
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
			self.metric, &self.pricing_options, MarketGeneral::MarketRemote(CurrNewMarket::New)
		    ).await;
		    *portf += new_trade_price;  // portfolio update
		},
	    }

	    


	}

	
    }

}
