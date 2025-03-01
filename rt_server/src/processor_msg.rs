/// messages for the Actor references.
/// 

use crate::market::{CurrNewMarket, MarketType};
use crate::ao_trade::AOTrade;
use crate::trade::TradeRep;
use crate::portfolio::PortfolioType;
use ractor::ActorRef;


/// message that the new processor receives
#[derive(Debug, Clone)]
pub enum ProcessorMiddleMessage {
    NewTrade(AOTrade),  // message from trade producer
    NewMarket(MarketType),  // message from market handler
    Behind(TradeRep<AOTrade>),  // message from Processor_below, missing trades to calculate.
    // message from Bulk computation
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    // fourth: market reference on which these trades were computed.
    BulkReceive(
	(TradeRep<AOTrade>, PortfolioType, TradeRep<AOTrade>, CurrNewMarket)
    ),
    // message from the processor above.
    NewTradePortfolio(
	(TradeRep<AOTrade>, PortfolioType, CurrNewMarket, ActorRef<ProcessorMiddleMessage>)
    ),
}


#[derive(Debug, Clone)]
pub enum ProcessorBulkMessage {
    NewBulk((CurrNewMarket, TradeRep<AOTrade>, ActorRef<ProcessorMiddleMessage>)),
    Abandon,  // TODO: WHAT TO DO W/ THIS???
}
