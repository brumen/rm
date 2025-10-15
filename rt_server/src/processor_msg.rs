/// messages for the Multiple Actor references.
///

use crate::market::MarketType;
use crate::trade::TradeRep;
use crate::portfolio::PortfolioType;
use ractor::ActorRef;


/// message that the new processor receives
#[derive(Debug, Clone)]
pub enum ProcessorMiddleMessage<'a, T> {
    NewTrade(T),  // message from trade producer
    NewMarket(&'a MarketType),  // message from market handler
    Behind(Vec<String>),  // message from Processor_below, missing trades to calculate.
    // message from Bulk computation
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    // fourth: market reference on which these trades were computed.
    BulkReceive(
	(Vec<String>, PortfolioType, Vec<String>, &'a MarketType)
    ),
    // message from the processor above.
    NewTradePortfolio(
	(TradeRep<T>, PortfolioType, &'a MarketType, ActorRef<ProcessorMiddleMessage<'a, T>>)
    ),
}


#[derive(Debug, Clone)]
pub enum ProcessorBulkMessage<'a, T> {
    // is a triple - first is the market type, a reference to a market.
    //    second - is a vector of trades that need to be computed.
    //    third - an actor processing ProcessorMiddleMessage<T>
    NewBulk(
        (&'a MarketType, Vec<String>, ActorRef<ProcessorMiddleMessage<'a, T>>)
    ),
    Abandon,  // TODO: WHAT TO DO W/ THIS???
}
