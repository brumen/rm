// middle processor, sits between 2 new processors

use std::sync::Arc;
use tracing::{info, warn, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::market::{MarketType, MarketSwitching, AllMarkets, };
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric};
use crate::process_trade::ProcessTradeValue;
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage};
use crate::trade::{TradeRep, BaseTrade};
//use crate::ao_trade::AOTrade;


#[derive(Debug)]
pub(crate) struct ProcessorMiddle<T>{
    pub(crate) metric: PricingMetric,
    pub(crate) pricing_options: MarketPricingOptions,
    pub(crate) processor_name: String,
    pub processor_below: ActorRef<ProcessorMiddleMessage<T>>,  // processor below
    pub processor_bulk: ActorRef<ProcessorBulkMessage<T>>,  // bulk processor ref.
    pub r_client: Option<reqwest::Client>,
    pub(crate) all_markets: Arc<AllMarkets>,
}


#[derive(Debug, Clone)]
pub enum ProcessorMiddleState {
    CalculatingSingle,  // when bulk has finished and we're only calculating single trades.
    CalculatingBulk,  // when we're still calculating bulk
    CalculatingBulkMarketSwitch,  // we are calculating bulk, but market
    // switched in the meantime, so the old calculating is not valid anymore
    Idle,
}


impl<T> MarketSwitching for ProcessorMiddle<T> {

    fn all_markets(&self) -> std::sync::Arc<AllMarkets> {
	self.all_markets.clone()
    }

    fn r_client(&self) ->  Option<&reqwest::Client> {
        self.r_client.as_ref()
    }

    fn market_endpoint(&self) -> String {
	format!("http://{0}/market", self.pricing_options.market_server.clone())
    }
}

impl<T> Decoder for ProcessorMiddle<T> {}


// TODO: IMPLEMENT RestPricerSpark HERE MISSING

#[async_trait]
impl<T> Actor for ProcessorMiddle<T>
where
    T: Sync + Send + 'static + Clone + BaseTrade + std::fmt::Debug + ProcessTradeValue
{
    type Msg = ProcessorMiddleMessage<T>;
    // first argument is list of trades,
    //   second is the list of trades that didnt price correctly
    //   third is the current portfolio result of correctly pricing trades.
    //   fourth is the computation state.
    type State = (TradeRep<T>, TradeRep<T>, PortfolioType, ProcessorMiddleState, MarketType);
    type Arguments = ();

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
	info!(
	    "Processor {}: Starting.", self.processor_name,
	);

        Ok(
	    (
		TradeRep::<T>::default(),
		TradeRep::<T>::default(),
		PortfolioType::default(),
		ProcessorMiddleState::Idle,
                MarketType::new(self.processor_name.clone()),
	    )
	)
    }

    //#[instrument]
    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	let (trade_l, trades_non_pricing, portf, pns, market) = state;

	let pns_old = (*pns).clone();  // otherwise we cant match

	info!(
	    "Processor: {}. State: {:?}", self.processor_name, pns_old,
	);

	match (message, pns_old) {
	    (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::CalculatingSingle) => {
		//match pns {  // what is the processor doing right now.
		//    ProcessorMiddleState::CalculatingSingle(_market) => {

		// add the trade to the new portfolio and
		//   attempt again.
		info!(
		    "Processor {}: CalculatingSingle, Calculating single trade {}.",
                    self.processor_name,
                    new_trade.id()
		);
		let new_trade_price = new_trade.value_by_metric2(
		    self.metric,
		    &self.pricing_options,
		    market.clone(),
		).await;
		*portf += new_trade_price;  // portfolio update
		*trade_l += &new_trade;  // we add the trade to the list.

		// we send the computed portfolio & trades to the current processor
		//   hoping that we are ahead.
		info!(
		    "Processor {}, CalculatingSingle: sending potential portfolio to below.",
		    self.processor_name,
		);
		self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), market.clone(), myself)
		    )
		)?;
	    },

	    (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::Idle) => {
		// start the new portfolio construction.
		info!(
		    "Processor {}, CalculatingSingle: got NewTrade, starting bulk computation.",
		    self.processor_name,
		);

		*trade_l += &new_trade;
		self.processor_bulk.send_message(
		    ProcessorBulkMessage::NewBulk(
			(market.clone(), trade_l.clone(), myself)
		    )
		)?;
		*pns = ProcessorMiddleState::CalculatingBulk;
	    },

	    (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::CalculatingBulk | ProcessorMiddleState::CalculatingBulkMarketSwitch) => {
		info!(
		    "Processor {}, CaluclatingBulk: got new trade.",
		    self.processor_name,
		);
		*trade_l += &new_trade;  // we add the trade to the list.
		let new_trade_price = new_trade.value_by_metric2(
		    self.metric,
		    &self.pricing_options,
		    market.clone(),
		).await;
		*portf += new_trade_price;  // portfolio update
		// send downstream the updated portfolio
                info!(
                    "Processor {}, CalculatingBulk: Sending new portfolio below",
                    self.processor_name,
                );

                // TODO: CHECK IF THIS SHOULD BE HANDLED???
                let _ = self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), market.clone(), myself)
		    )
		);
	    },

	    (ProcessorMiddleMessage::NewMarket(_new_market), _) => {
		panic!(
		    "Processor {}: received market message. THIS SHOULDNT HAPPEN!",
		    self.processor_name,
		);
	    },

	    // this only comes from processor below
	    (ProcessorMiddleMessage::Behind(trades_behind), ProcessorMiddleState::CalculatingSingle) => {
		// we are behind trades behind the current processor

		// TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
		// TRADES ARE BEHIND,
		//    < 10 -> continue in single mode
		//    > 10 -> continue in bulk mode.
		if trades_behind.is_empty() {
		    // this processor is ahead, reset the
		    //    new processor to the new default state.
		    info!(
			"Processor {}, Behind: Accepted portfolio from market below. Resetting",
			self.processor_name,
		    );

		    // *portf = PortfolioType::default();
		    let mkt_above = market
		        .next_market(&self.all_markets)
		        .expect("No next market. PROBLEM!!");

		    info!(
			"Processor {}: Switching markets {} <- {}. Going to Idle.",
			self.processor_name, market, mkt_above,
		    );
                    self.switch_market(market).await?;
		    *pns = ProcessorMiddleState::Idle;

		} else {
		    // we are still behind the below processor.
		    //   we add the trades to the trade list, and
		    //   send it to the bulk processor.
		    info!(
			"Processor {}, Behind: {} trades.",
			self.processor_name, trades_behind.len(),
		    );
		    *trade_l += &trades_behind;
                    info!(
                        "Processor {}, Behind: Sending bulk compute.", market
                    );
		    self.processor_bulk.send_message(
			ProcessorBulkMessage::NewBulk(
			    (market.clone(), trades_behind.clone(), myself)
			)
		    )?;
		    *pns = ProcessorMiddleState::CalculatingBulk;
		}
	    },

	    (
		ProcessorMiddleMessage::Behind(trades_behind),
		ProcessorMiddleState::CalculatingBulk | ProcessorMiddleState::CalculatingBulkMarketSwitch
	    ) => {
		if trades_behind.is_empty() {
		    // new processor is ahead, reset the
		    //    new processor to the new default state.

                    //*portf = PortfolioType::default();

		    info!(
			"Processor {}, Calculating bulk: Received confirmation \
			 that lower processor accepted portfolio. Defaulting.",
			self.processor_name
		    );
                    info!(
                        "{}, CalculatingBulk|Switch, Behind: switching markets.",
                        self.processor_name,
                    );
		    // switch markets & put it in CalculatingBulkMarketSwitch
		    self.switch_market(market).await?;
		    *pns = ProcessorMiddleState::CalculatingBulkMarketSwitch;

		} else {
		    // new processor is behind, calculate the remaining trades.
		    // we are still behind the current processor.
		    // TODO: MAYBE WE CAN DIFFERENTIATE ON HOW MANY TRADES BEHIND???
		    info!(
			"Processor {}, Calculating bulk: Lower processor \
			 did not accept the portfolio. Adding trades.",
			self.processor_name,
		    );

		    *trade_l += &trades_behind;
                    info!(
                        "Processor {}, CalculatingBulk: Sending for bulk compute.",
                        self.processor_name,
                    );

		    self.processor_bulk.send_message(
			ProcessorBulkMessage::NewBulk(
		 	    (market.clone(), trade_l.clone(), myself)
			)
		    )?;
		}
	    },

	    (ProcessorMiddleMessage::Behind(trades_behind), ProcessorMiddleState::Idle) => {
		if !trades_behind.is_empty() {
		    info!(
			"Processor {}, Idle: Lower processor accepted portfolio. \
			 Starting new computations.",
			self.processor_name,
		    );

		    self.processor_bulk.send_message(
			ProcessorBulkMessage::NewBulk(
			    (market.clone(), trades_behind.clone(), myself)
			)
		    )?;
		    *pns = ProcessorMiddleState::CalculatingBulk;
		    //*trade_l += &trades_behind;
		} // else dont do anything.
	    },


	    // bcp = (new_trade_l, computed_portf, offending_trades, _bulk_market)
	    (ProcessorMiddleMessage::BulkReceive(_), ProcessorMiddleState::Idle) => {
		// Important: This Souldnt happen.
		// TODO: CHECK WHY THIS IS THE CASE???
		warn!(
                    "Processor {}, Idle: Ignoring bulk receive. Should not happen.",
                    self.processor_name,
                );
	    },

	    (
		ProcessorMiddleMessage::BulkReceive((new_trade_l, computed_portf, offending_trades, _bulk_market)),
		ProcessorMiddleState::CalculatingBulk
	    ) => {
		// result of computation has arrived.
		// TODO: FINISH THIS HERE - what to do w/ offending trades???
		//    Nothing for now.
		info!(
		    "Processor {}, CalculatingBulk: received response from bulk. \
		     Normal case. Going to CalculatingSingle.",
		    self.processor_name,
		);

		*portf += &computed_portf;
		*trade_l += &new_trade_l;
		*trades_non_pricing += &offending_trades;
		self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), market.clone(), myself)
		    )
		)?;
		*pns = ProcessorMiddleState::CalculatingSingle;
	    },

	    (
		ProcessorMiddleMessage::BulkReceive((new_trade_l, computed_portf, _offending_trades, _bulk_market)),
		ProcessorMiddleState::CalculatingSingle
	    ) => {
		// result of computation has arrived.
		//  add it to the computation
		// TODO: WHAT TO DO W/ OFFENDING TRADES???

		info!("Processor {}, BulkReceive: Sending to processor below.",
		      self.processor_name,
		);
		*portf = computed_portf;
		*trade_l += &new_trade_l;
		self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), market.clone(), myself)
		    )
		)?;
	    },

	    (ProcessorMiddleMessage::BulkReceive(_), ProcessorMiddleState::CalculatingBulkMarketSwitch) => {
		// ignore the message from bulk receive,
		info!(
		    "Processor {}, CalculatingBulkMarektSwitch: \
                     Ignoring message from bulk receive as market was switched.",
		    self.processor_name,
		);
	    }

	    // receiving a mesage from the processor above
	    // ntp = (trades, potential_portfolio, market, upstream_processor)
	    (ProcessorMiddleMessage::NewTradePortfolio(ntp), ProcessorMiddleState::Idle) => {
		// just pass it to the processor below, dont do anything else
		info!(
		    "Processor {}, Idle: Switching market",
		    self.processor_name,
		);

                let (ref potential_trades, ref potential_portfolio, ref _new_market, ref upstream_processor) = ntp;

		// switch markets as well
		self.switch_market(market).await?;
		info!(
		    "Processor {}, Idle: passing portfolio to lower processor",
		    self.processor_name,
		);
		self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(ntp.clone())
		)?;

                // acknowledge to the sending processor that it was accepted.
                *trade_l = potential_trades.clone();
                *portf = potential_portfolio.clone();  // TODO: CHECK HERE AND ABOVE

                // TODO: CHECK IF THIS SHOULD BE HANDLED???
                let _ = upstream_processor.send_message(
                   ProcessorMiddleMessage::Behind(TradeRep::<T>::default())
                );

	    },

	    // TODO: CHECK HERE IF ...MarketSwitch should be handled separately.
	    (ProcessorMiddleMessage::NewTradePortfolio(ntp), ProcessorMiddleState::CalculatingBulk | ProcessorMiddleState::CalculatingBulkMarketSwitch) => {
		// we got new portfolio, but we are in the process of computing the portfolresult of computation has arrived.
		let (potential_trades, potential_portfolio, _new_market, upstream_processor) = ntp;

		//if new_market.is_later_than(market) {  // TODO: THIS IS NOT SUFFICIENT CONDITION
		// replace the portfolio, and pass it down

		// TODO: ALSO, SHOULDNT WE NOTIFY THE BULK PROCESSOR THAT WE ARE ABANDONING THE ATTEMPT.
		// TODO: WRONG - IMPLEMENT > JUST FOR REFERENCES!!!
		let new_behind_curr = potential_trades.clone() - &potential_trades;

		if new_behind_curr.is_empty() {
		    info!(
			"Processor {}, CalculatingBulk: received new portfolio, \
                         was ahead, sending portfolio to processor below",
			self.processor_name,
		    );

		    self.processor_below.send_message(
			ProcessorMiddleMessage::NewTradePortfolio(
			    (trade_l.clone(), portf.clone(), market.clone(), myself)
			)
		    )?;

                    info!(
			"Processor {}, CalculatingBulk|Switch: New portfolio, Switching market.",
			self.processor_name,
		    );

                    match self.r_client() {
                        None => {
                            *market = _new_market;
                        },
                        Some(_) => {
                            self.switch_market(market).await?;
                        },
                    }

		    // set the state of this processor to the state being sent.
		    *portf = potential_portfolio;
		    *trade_l = potential_trades;
		    *pns = ProcessorMiddleState::CalculatingBulkMarketSwitch;

		} else {
		    info!(
			"Processor {}, CalculatingBulk: received new portfolio, but was behind. Ignoring.",
			self.processor_name,
		    );
		}
                // send upstream a message that the portfolio is accepted.
                // TODO: CHECK IF THIS SHOULD BE BETTER HANDLED
                let _ = upstream_processor.send_message(
                    ProcessorMiddleMessage::Behind(new_behind_curr)
                );
	    },

	    (ProcessorMiddleMessage::NewTradePortfolio(ntp), ProcessorMiddleState::CalculatingSingle) => {
		// result of computation has arrived.
		//  add it to the computation
		// TODO: FINISH THIS HERE!!!

		info!(
		    "Processor {}. CalculatingSingle: Received new trade portfolio",
		    self.processor_name,
		);

		let (potential_trades, potential_portfolio, _new_market, upstream_processor) = ntp;  // new trade portfolio

		// TODO: WRONG - IMPLEMENT > JUST FOR REFERENCES!!!
		let new_behind_curr = trade_l.clone() - &potential_trades;
                info!(
                    "Processor {}, NewPortfolio: My trades: {}, Potential trades: {}, New trades: {}, New portf: {}",
                    self.processor_name, trade_l.len(), potential_trades.len(), new_behind_curr.len(), potential_portfolio.len(),
                );

		if new_behind_curr.is_empty() {
		    // replace the portfolio and trades

		    info!(
			"Processor {}, CalculatingSingle: Portfolio is good, accepting \
			 it and passing to processor below",
			self.processor_name,
		    );
		    *portf = potential_portfolio;
		    *trade_l += &potential_trades;
		    // TODO: HOW ABOUT pns ???
		    self.processor_below.send_message(
			ProcessorMiddleMessage::NewTradePortfolio(
			    (trade_l.clone(), portf.clone(), market.clone(), myself)
			)
		    )?;

                    info!(
                        "Processor {}, NewPortfolio: switching markets",
                        self.processor_name,
                    );

                    match self.r_client() {
                        Some(_) => {
                            self.switch_market(market).await?;
                        },
                        None => {
                            *market = _new_market;
                        },
                    }
		}
                // sending upstream that we are done.
                // TODO: CHECK IF THIS SHOULD BE BETTER HANDLED
                let _ = upstream_processor.send_message(
                    ProcessorMiddleMessage::Behind(new_behind_curr)
                );
	    },
	}
	Ok(())
    }
}
