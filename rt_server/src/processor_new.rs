use ractor::{async_trait, cast, Actor, ActorProcessingErr, ActorRef};

use std::sync::{Arc, Mutex};
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
pub struct ProcessorNew<'a, ReductionType>{
    metric: PricingMetric,
    pricing_options: &'a MarketPricingOptions,
    all_trades: Arc<Mutex<TradeRep<ReductionType>>>,
    curr_portfolio: PortfolioType,
}

/// This is the types of message [PingPong] supports
#[derive(Debug, Clone)]
pub enum ProcessorNewMessage {
    NewTrade(Trade),
    NewMarket(Market),
}

pub enum ProcessorNewState {
    Calculating(Market),
    Idle(Market),
}


#[async_trait]
impl<'a, ReductionType> Actor for ProcessorNew<'a, ReductionType> {
    type Msg = ProcessorNewMessage;
    type State = ProcessorNewState;
    type Arguments = ();  // initialization args.

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        // startup the event processing
        cast!(myself, Message::Ping)?;  // first message
        // create the initial state
        Ok(0u8)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> 
    {
	match message {
	    NewTrade(new_trade) => {},
	    NewMarket(new_market) => {
		i
	    },
	}

    }

    async fn _get_trades_from_recv(
        &self,
        trade_receiver: &mut Receiver<<Self as TradeReduce>::ReductionType>,
    ) -> TradeRep<<Self as TradeReduce>::ReductionType>
    {
        let mut new_trades = TradeRep::<<Self as TradeReduce>::ReductionType>::default();

        while let Ok(trade) = trade_receiver.try_recv() {
            new_trades += &trade;
        }
        new_trades
    }

}
