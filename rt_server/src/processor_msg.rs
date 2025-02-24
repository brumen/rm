/// messages for the Actor references.
/// 

// #[derive(Debug, Clone)]
// pub enum ProcessorNewMessage {
//     NewTrade(AOTrade),
//     NewMarket(MarketType),
//     Behind(i32),  // message from ProcessorCurr
//     BulkReceive((TradeRep<AOTrade>, PortfolioType)),  // message from Bulk computation
// }

// pub enum ProcessorCurrMessage {
//     NewTrade(AOTrade),
//     NewTradePortfolio((TradeRep<AOTrade>, PortfolioType)),
// }

use crate::market::MarketType;
use crate::ao_trade::AOTrade;
use crate::trade::TradeRep;
use crate::portfolio::PortfolioType;
use ractor::ActorRef;


// #[derive(Debug, Clone)]
// pub enum ProcessorBulkMessage {
//     NewBulk((MarketType, TradeRep<AOTrade>)),
//     Abandon,
// }

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


#[derive(Debug, Clone)]
pub enum ProcessorBulkMessage {
    NewBulk((MarketType, TradeRep<AOTrade>, ActorRef<ProcessorMiddleMessage>)),
    Abandon,  // TODO: WHAT TO DO W/ THIS???
}


