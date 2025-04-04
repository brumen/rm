/// messages for the Actor references.
///

use crate::market::MarketType;
//use crate::ao_trade::AOTrade;
use crate::trade::TradeRep;
use crate::portfolio::PortfolioType;
use ractor::ActorRef;


/// message that the new processor receives
#[derive(Debug, Clone)]
pub enum ProcessorMiddleMessage<T> {
    NewTrade(T),  // message from trade producer
    NewMarket(MarketType),  // message from market handler
    Behind(TradeRep<T>),  // message from Processor_below, missing trades to calculate.
    // message from Bulk computation
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    // fourth: market reference on which these trades were computed.
    BulkReceive(
	(TradeRep<T>, PortfolioType, TradeRep<T>, MarketType)
    ),
    // message from the processor above.
    NewTradePortfolio(
	(TradeRep<T>, PortfolioType, MarketType, ActorRef<ProcessorMiddleMessage<T>>)
    ),
}


#[derive(Debug, Clone)]
pub enum ProcessorBulkMessage<T> {
    NewBulk((MarketType, TradeRep<T>, ActorRef<ProcessorMiddleMessage<T>>)),
    Abandon,  // TODO: WHAT TO DO W/ THIS???
}
