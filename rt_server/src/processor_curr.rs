use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef};

use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info, instrument, warn};

use crate::market::{CurrNewMarket, MarketType, TradeMarketDiscovery};
use crate::portfolio::{PortfolioType, PricingResults};
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::process_trade::{ObtainMarket, ProcessTradeValue};
use crate::trade::{BaseTrade, TradeDirection, TradeReduce, TradeRep};


/// ProcessorNew is actor representation of the
///    new processor.
pub struct ProcessorCurr<'a, ReductionType>{
    metric: PricingMetric,
    pricing_options: &'a MarketPricingOptions,
    all_trades: TradeRep<ReductionType>,
    curr_portfolio: PortfolioType,
}

pub enum ProcessorCurrMessage {
    NewTrade(Trade),
    NewPortfolio(PortfolioType),
}

pub enum ProcessorCurrState {
    
}


#[async_trait]
impl<'a, ReductionType> Actor for ProcessorCurr<'a, ReductionType>
where
    ReductionType: std::fmt::Debug
{
    type Msg = ProcessorCurrMessage;
    type State = u8;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	// TODO: FINISH THIS HERE!!!
	self.portfolio_sender = args.portfolio_sender;
	
	Ok(0u8)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> 
	//     mut curr_trade_receiver: Receiver<<Self as TradeReduce>::ReductionType>,
    //     metric: PricingMetric,
    //     pricing_options: &MarketPricingOptions,
    //     all_trades: Arc<Mutex<TradeRep<<Self as TradeReduce>::ReductionType>>>,
    //     curr_portfolio: Arc<Mutex<PortfolioType>>,
    //     curr_portfolio_sender2: &mut Sender<(
    //         PortfolioType,
    //         TradeRep<<Self as TradeReduce>::ReductionType>,
    //     )>,
    // ) -> impl std::future::Future<Output = ()> + Send
    // where
    //     <Self as TradeReduce>::ReductionType: std::fmt::Debug + ProcessTradeValue,
    {
        match message {
	    NewTrade(trade) => {

		// curr_trade_receiver.recv().await {
                info!("CURR processor: Processing trade {:?}.", trade);
                let valued_trade = self
                    ._process_trade(&trade, self.metric, self.pricing_options, CurrNewMarket::Current)
                    .await;
                debug!("CURR processor: Trade value: {:?}", valued_trade);

		// updating the portfolio
                *self.all_trades += &trade;
                *self.curr_portfolio += valued_trade;

                let (curr_p2, all_t2) = {
                    let c3 = self.curr_portfolio.clone();
                    let t3 = TradeRep(self.all_trades.clone());
                    (c3, t3)
                };
                let _ = curr_portfolio_sender2.send((curr_p2, all_t2)).await;
            },

	    NewPortfolio(new_portfolio) => {
		// Switch portfolios 
		if self._possible_portf_switch(new_portfolio, ) {
		    // replace the portfolio w/ new and trades
		    self.curr_portfolio = new_portfolio;
		    self.all_trades = new_trades;  // TODO: FIINISH HERE!!!
		    // TODO: NEED TO BE SOME CASTING!!!
		}
	    }
        }
	Ok(())
    }


    /// future that listens to new_portfolio_receiver,
    ///    and if it receives relevant portfolio,
    ///    switches curr <- new portfolio.
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
        let (new_p, new_trades) = new_portfolio; 
        let all_l = all_trades_2.len();
        let new_l = new_trades.len();
        debug!(
            "Trades from NEW processor: {}. Trades on CURR processor: {}",
            new_l, all_l,
        );

        let behind = (all_l as i32) - (new_l as i32); // how far behind is the
        let _ = accepted_sender.send(behind).await;
        if new_l >= all_l {
            // new processor is further ahead
            info!(
                "Switching curr_p <- new_p: Nb trades = {}",
                new_trades.len()
            );
            let _ = curr_portfolio_sender
                .send((new_p.clone(), TradeRep(new_trades.clone())))
                .await;
	    
            self._switch_all_markets().await; // curr <- new, new <- fut
            *curr_portfolio_2.lock().unwrap() = new_p;
            *all_trades_2.lock().unwrap() += &new_trades;
        } else {
            info!("New portfolio behind old one, not switching.");
        }
    }
}
