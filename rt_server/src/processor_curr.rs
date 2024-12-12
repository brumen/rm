use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef};

use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info, instrument, warn};

use crate::ao_trade::AOTrade;
use crate::market::{CurrNewMarket, MarketType, TradeMarketDiscovery};
use crate::portfolio::{PortfolioType, PricingResults};
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::process_trade::{ObtainMarket, ProcessTradeValue};
use crate::trade::{BaseTrade, TradeDirection, TradeReduce, TradeRep};
use crate::processor_new::{ProcessorNew, ProcessorNewMessage};

/// ProcessorNew is actor representation of the
///    new processor.
pub struct ProcessorCurr<'a>{
    metric: PricingMetric,
    pricing_options: &'a MarketPricingOptions,
    new_processor: ActorRef<ProcessorNew<'a>>,
    // TODO: PUBLISHER IS MISSING!!!
}

pub enum ProcessorCurrMessage {
    NewTrade(AOTrade),
    NewTradePortfolio((TradeRep<AOTrade>, PortfolioType)),
}

pub enum ProcessorCurrState {
    
}

pub trait SwitchPortfolio<'a, ReductionType> {

    fn procs_new(self) -> ProcessorNew<'a, ReductionType>;

    async fn _switch_all_markets(self);

    /// number of trades that new processor is behind
    ///   current processor
    async fn _new_behind_curr(
        &self,
	new_portfolio: (PortfolioType, TradeRep<ReductionType>),
    ) -> i32 {
        let (new_p, new_trades) = new_portfolio; 
        let all_l = self.all_trades.len();  // trades on curr processor
        let new_l = new_trades.len();  // trades on new processor

	// how far behind is the new processor
        (all_l as i32) - (new_l as i32);
        // let _ = accepted_sender.send(behind).await;
    }

    async fn _possible_portf_switch(
        &self,
	new_portfolio: (PortfolioType, TradeRep<ReductionType>),
        all_trades_2: TradeRep<ReductionType>,
        curr_portfolio_2: PortfolioType,
        accepted_sender: Sender<i32>,
        curr_portfolio_sender: &mut (
            PortfolioType,
            TradeRep<ReductionType>,
        ),
    ) {
        // debug!(
        //     "Trades from NEW processor: {}. Trades on CURR processor: {}",
        //     new_l, all_l,
        // );

        let behind = self._new_behind_curr(new_portfolio).await;

	cast!(
	    self.procs_new(),
	    ProcessorNewMessage::Behind(behind)
	);

        // if new_l >= all_l {
	if behind < 0 {
            // new processor is further ahead
            // info!(
            //     "Switching curr_p <- new_p: Nb trades = {}",
            //     new_trades.len()
            // );

            // let _ = curr_portfolio_sender
            //     .send((new_p.clone(), TradeRep(new_trades.clone())))
            //     .await;
	    let (new_p, new_trades) = new_portfolio;
	    cast!(
		self.publisher(),
		(new_p.clone(), TradeRep(new_trades.clone())),
	    );
	    
            self._switch_all_markets().await; // curr <- new, new <- fut

            self.curr_portfolio = new_p;
            self.all_trades += &new_trades;

        } else {
            info!(
		"New portfolio behind old one by {:?}, not switching.",
		behind
	    );
        }
    }

}


impl<'a, ReductionType> SwitchPortfolio<ReductionType> for ProcessorCurr<'a, ReductionType> {

    fn procs_new(self) -> ProcessorNew<'a,ReductionType> {
	self.processor_new
    }
}


impl Actor for ProcessorCurr<'_> {
    type Msg = ProcessorCurrMessage;
    // state is a tuple of current trades,
    //    and current portfolio.
    type State = (TradeRep<AOTrade>, PortfolioType);  
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let initial_trades = TradeRep::<ReductionType>::default();
	let initial_curr_portf = PortfolioType::default();

	Ok((initial_trades, initial_curr_portf))
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
	let (trades, portf) = state;

        match message {
	    ProcessorCurrMessage::NewTrade(trade) => {

		// curr_trade_receiver.recv().await {
                info!("CURR processor: Processing trade {:?}.", trade);
                let valued_trade = self
                    ._process_trade(&trade, self.metric, self.pricing_options, CurrNewMarket::Current)
                    .await;
                debug!("CURR processor: Trade value: {:?}", valued_trade);

		// updating the portfolio
		*trades += &trade;
		*portf += valued_trade;

		// TODO: send to the publisher actor
            },

	    ProcessorCurrMessage::NewTradePortfolio((new_trades, new_portfolio)) => {
		// Switch portfolios 
		if self._possible_portf_switch(new_portfolio, ) {
		    // replace the portfolio w/ new and trades
		    portf = &mut new_portfolio;
		    trades = &mut new_trades;
		}
	    }
        }

	// sends to publisher actor
	self.publisher.cast(portf);
	
	Ok(())
    }
}
