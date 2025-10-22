/// messages for the Multiple Actor references.
use ractor::ActorRef;

use crate::portfolio::PortfolioType;


/// message that the new processor receives
#[derive(Debug, Clone)]
pub enum ProcessorMiddleMessage<MT> {
    NewTrade(String),  // message from trade producer, trade id.
    NewMarket(MT),  // message from market handler, market_name
    Behind(Vec<String>),  // message from Processor_below, missing trades to calculate.

    // message from Bulk computation
    // first elt: all trades,
    // second: portfolio from computed trades
    // third: offending trades.
    // fourth: market reference on which these trades were computed.
    BulkReceive(
	(Vec<String>, PortfolioType, Vec<String>, String)
    ),

    // message from the processor above.
    // elements:
    //    1st elt: trades for which portfolio was computed.
    //    2nd elt: portfolio:
    //    3rd market for which it was computed.
    //    4th actor where this was sent from.
    NewTradePortfolio(
	(Vec<String>, PortfolioType, String, ActorRef<ProcessorMiddleMessage<MT>>)
    ),
    // processing stat:
    //   1st arg: processor name
    //   2nd arg: when the events ocurred.
    //   3rd arg: cumulative number of trades processed.
    ProcessingStat((String, chrono::NaiveDateTime, usize)),
}


#[derive(Debug, Clone)]
pub enum ProcessorBulkMessage<MT> {
    // is a triple
    //    first is the market type, a name of the market
    //    second - is a vector of trades that need to be computed.
    //    third - an actor processing ProcessorMiddleMessage
    NewBulk(
        (String, Vec<String>, ActorRef<ProcessorMiddleMessage<MT>>)
    ),
    Abandon,  // TODO: WHAT TO DO W/ THIS???
}
