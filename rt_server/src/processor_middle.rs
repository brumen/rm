// middle processor, sits between 2 new processors
use std::sync::Arc;
use tracing::{info, warn, error};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use crate::all_markets::AllMarkets;
use crate::portfolio::{PortfolioType, PmPortfolio};
use crate::pricer::{PricingMetric, PriceTrade};
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage, TradesLocal};
use crate::trade::{TradeRep, BaseTrade};
use crate::market::MarketTypeT;


// T is mnemonic for trade type, MT is mnemonic for market type
pub(crate) struct ProcessorMiddle<T, MT> {
    pub(crate) processor_name: String,
    pub processor_below: ActorRef<ProcessorMiddleMessage<String>>,
    pub processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
    pub(crate) all_markets: Arc<AllMarkets<Arc<MT>>>,
    pub(crate) all_trades: Arc<TradeRep<T>>,
}


impl<T, MT> ProcessorMiddle<T,MT>
where
    MT: MarketTypeT,
    MT::MP : Clone,
{
    pub(crate) fn new(
        processor_name: String,
        processor_below: ActorRef<ProcessorMiddleMessage<String>>,
        processor_bulk: ActorRef<ProcessorBulkMessage<String>>,
        all_trades: Arc<TradeRep<T>>,
        all_markets: Arc<AllMarkets<Arc<MT>>>,
    ) -> Self {

        Self {
            processor_name,
            processor_below,
            processor_bulk,
            all_trades,
            all_markets,
        }
    }
}


#[derive(Debug, Clone)]
pub enum ProcessorMiddleState {
    CalculatingSingle,  // when bulk has finished and we're only calculating single trades.
    CalculatingBulk,  // when we're still calculating bulk
    CalculatingBulkMarketSwitch,  // we are calculating bulk, but market
    // switched in the meantime, so the old calculating is not valid anymore
    Idle,
}


#[async_trait]
impl<T, MT> Actor for ProcessorMiddle<T, MT>
where
    T: Sync + Send + 'static + Clone + BaseTrade + PriceTrade<MT>,
    MT: Send + Sync + MarketTypeT + 'static,
    MT::MP : Clone,
{
    type Msg = ProcessorMiddleMessage<String>;  // dyn MarketTypeT<MP=MP>>;
    // state of the processor middle:
    //   1st arg: list of trades,
    //   2nd arg: list of trades that didnt price correctly
    //   3rd: third is the current portfolio result for each pricing metric of correctly pricing trades.
    //   fourth is the computation state.
    //   fifth is the market that the processor is operating on.
    //      for remote pricing markets only market_name is fine,
    //      for local markets, the name and the market structure.
    //   6th: list of metrics that the system is operating on.
    type State = (TradesLocal, TradesLocal, PmPortfolio, ProcessorMiddleState, Option<String>, Vec<PricingMetric>);
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
		TradesLocal::new(),
		TradesLocal::new(),
		PmPortfolio::new(),  // no portfolio
		ProcessorMiddleState::Idle,
                None,  // original market, none
                vec![],  // no metrics at first
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

	let (trade_l, trades_non_pricing, portf, pns, market, pricing_metrics) = state;  // market is the market where we're operating
	let pns_old = (*pns).clone();  // otherwise we cant match

	info!(
	    "Processor: {}. State: {:?}", self.processor_name, pns_old,
	);

	match (message, pns_old) {
	    (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::CalculatingSingle) => {
                let trade_info = self.all_trades.get(&new_trade).ok_or("Trade not found")?;
                info!(
		    "Processor {}: CalculatingSingle, Calculating single trade {}.",
                    self.processor_name,
                    trade_info.id()
		);

                let Some(real_market) = market else {
                    warn!("Does not have market. Ignoring.");
                    return Ok(());
                };

                let Some(market_info) = self.all_markets.get(real_market) else {
                    warn!("Could not get market {}. Continuing.", real_market);
                    return Ok(());
                };
                let real_trade = trade_info.value();

                for pm in pricing_metrics {
                    let new_trade_price_pm = real_trade.value_by_metric(
		        *pm,
		        market_info.clone(),
		    ).await;
                    let portf_pm = portf.get(*pm);  // only portfolio for that metric.
                    *portf_pm += new_trade_price_pm;  // portfolio update
                }
                trade_l.insert(new_trade);  // we add the trade to the list.

		// we send the computed portfolio & trades to the processor below
		//   hoping that we are ahead.
		info!(
		    "Processor: {}, State: CalculatingSingle: sending potential portfolio to processor below.",
		    self.processor_name,
		);
		self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), real_market.to_string(), myself)
		    )
		)?;
	    },

	    (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::Idle) => {
		// start the new portfolio construction.
		info!(
		    "Processor {}, CalculatingSingle: got NewTrade, starting bulk computation.",
		    self.processor_name,
		);

                trade_l.insert(new_trade);  //*trade_l += &new_trade;

                let Some(real_market) = market else {
                    warn!("Does not have real market. Ignoring.");
                    return Ok(());
                };

		self.processor_bulk.send_message(
		    ProcessorBulkMessage::NewBulk(
			(real_market.to_string(), trade_l.clone(), myself, pricing_metrics.clone())
		    )
		)?;
		*pns = ProcessorMiddleState::CalculatingBulk;
	    },

	    (ProcessorMiddleMessage::NewTrade(new_trade), ProcessorMiddleState::CalculatingBulk | ProcessorMiddleState::CalculatingBulkMarketSwitch) => {
		info!(
		    "Processor {}, CalculatingBulk|CalculatingBulkMarketSwitch: got new trade.",
		    self.processor_name,
		);

                let Some(real_market) = market else {
                    warn!("Does not have market. Ignoring.");
                    return Ok(());
                };

                if let Some(real_trade) = self.all_trades.get(&new_trade) {
                    if let Some(real_market) = self.all_markets.get(real_market) {
                        for pm in pricing_metrics {
                            let new_trade_price = real_trade.value_by_metric(
		                *pm,
		                real_market.clone(),
		            ).await;
                            let portf_pm = portf.get(*pm);
                            *portf_pm += new_trade_price;  // portfolio update
                        }

                        trade_l.insert(new_trade);
                    } else {
                        warn!("Could not get market {}", real_market);
                        trades_non_pricing.insert(new_trade);
                    }
                } else {
                    warn!("Could not get the representation of {}", new_trade);
                    trades_non_pricing.insert(new_trade);
                }

		// send downstream the updated portfolio
                info!(
                    "Processor {}, CalculatingBulk: Sending new portfolio below",
                    self.processor_name,
                );

                // TODO: CHECK IF THIS SHOULD BE HANDLED???
                let _ = self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), real_market.to_string(), myself)
		    )
		);
	    },

	    (ProcessorMiddleMessage::NewMarket(_new_market), _) => {
                let msg =  format!(
		    "Processor {}: received NewMarket. THIS SHOULDNT HAPPEN!",
		    self.processor_name,
		);
                error!(msg);  // log the error and panic
                panic!("{}", msg);
	    },

	    // this only comes from processor below
	    (ProcessorMiddleMessage::Behind(market_behind, trades_behind), ProcessorMiddleState::CalculatingSingle) => {
		// we are behind trades behind the current processor

		// TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
		// TRADES ARE BEHIND,
		//    < 10 -> continue in single mode
		//    > 10 -> continue in bulk mode.
		if trades_behind.is_empty() {
		    // this processor below is ahead, reset the
		    //    processor to the new Idle state.
		    info!(
			"Processor: {}, State: (Behind, CalculatingSingle): Accepted portfolio from market below. Going Idle.",
			self.processor_name,
		    );
		    *pns = ProcessorMiddleState::Idle;

		} else {
		    // we are still behind the below processor.
		    //   we add the trades to the trade list, and
		    //   send it to the bulk processor.
		    info!(
			"Processor {}, State: (Behind, CalculatingSingle): Behind: {} trades.",
			self.processor_name, trades_behind.len(),
		    );

                    trade_l.extend(trades_behind.clone());  // *trade_l += &trades_behind;

                    let Some(real_market) = market else {
                        warn!("Does not have market. Ignoring.");
                        warn!("{}: Destroying the market {}",
                              self.processor_name, market_behind,
                        );
                        self.all_markets.remove(&market_behind);
                        return Ok(());
                    };

                    if market_behind != *real_market {
                        warn!("{}: Destroying the market {}",
                              self.processor_name, market_behind,
                        );
                        self.all_markets.remove(&market_behind);
                    }

                    info!(
                        "Processor {}, Behind: Sending bulk compute.", real_market
                    );
		    self.processor_bulk.send_message(
			ProcessorBulkMessage::NewBulk(
			    (real_market.to_string(), trades_behind, myself, pricing_metrics.clone())
			)
		    )?;
		    *pns = ProcessorMiddleState::CalculatingBulk;
		}
	    },

	    (
		ProcessorMiddleMessage::Behind(market_behind, trades_behind),
		ProcessorMiddleState::CalculatingBulk | ProcessorMiddleState::CalculatingBulkMarketSwitch
	    ) => {
		if trades_behind.is_empty() {
		    // new processor is ahead, reset the
		    //    processor to the new default state.

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
		    //self.switch_market(market).await?;
		    *pns = ProcessorMiddleState::CalculatingBulkMarketSwitch;

		} else {
		    // the processor is behind the below processor, calculate the remaining trades.
		    // we are still behind the current processor.
		    // TODO: MAYBE WE CAN DIFFERENTIATE ON HOW MANY TRADES BEHIND???
		    info!(
			"Processor {}, State: CalculatingBulk|CalculatingBulkMarketSwitch : Lower processor \
			 did not accept the portfolio. Adding trades, removing the market_behind",
			self.processor_name,
		    );

                    trade_l.extend(trades_behind);  // *trade_l += &trades_behind;

                    // delete the market_behind if it's not market
                    warn!(
                        "Processor {} has market: None. Removing {} and Ignoring/Continuing.",
                        self.processor_name, market_behind,
                    );
                    let Some(real_market) = market else {
                        *market = Some(market_behind);
                        return Ok(());
                    };

                    // remove market_behind if not equal to current market here - should never happen
                    if *real_market != market_behind {
                        warn!("{}: Destroying market: {}",
                              self.processor_name, market_behind,
                        );
                        self.all_markets.remove(&market_behind);
                    }

                    info!(
                        "Processor {}, State: CalculatingBulk|CalculatingBulkMarketSwitch: Sending for bulk compute.",
                        self.processor_name,
                    );

		    self.processor_bulk.send_message(
			ProcessorBulkMessage::NewBulk(
		 	    (real_market.to_string(), trade_l.clone(), myself, pricing_metrics.clone())
			)
		    )?;
		}
	    },

	    (ProcessorMiddleMessage::Behind(market_behind, trades_behind), ProcessorMiddleState::Idle) => {
                // we're in idle state, and have received a rejected market.

                if !trades_behind.is_empty() {
		    info!(
			"Processor {}, State: Idle: Lower processor accepted portfolio. \
			 Starting new computations.",
			self.processor_name,
		    );

                    let Some(real_market) = market else {
                        warn!("Does not have market. Ignoring.");
                        return Ok(());
                    };

		    self.processor_bulk.send_message(
			ProcessorBulkMessage::NewBulk(
			    (real_market.to_string(), trades_behind.clone(), myself, pricing_metrics.clone())
			)
		    )?;
		    *pns = ProcessorMiddleState::CalculatingBulk;
		    trade_l.extend(trades_behind);  //*trade_l += &trades_behind;
		} else {
                    // remove the market_behind.
                    let Some(real_market) = market else {
                        *market = Some(market_behind);
                        return Ok(());
                    };

                    if market_behind != *real_market {
                        warn!("{}: Destroying market {}",
                              self.processor_name, market_behind,
                        );
                        self.all_markets.remove(&market_behind);
                    }
                }
	    },


	    // bcp = (new_trade_l, computed_portf, offending_trades, _bulk_market)
	    (ProcessorMiddleMessage::BulkReceive(_), ProcessorMiddleState::Idle) => {
		// Important: This Souldnt happen.
		// TODO: CHECK WHY THIS IS THE CASE???
		warn!(
                    "Processor {}, State: Idle: Ignoring bulk receive. Should not happen.",
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

                for (pm, computed_portf_pm) in computed_portf.iter() {
                    let portf_pm = portf.get(*pm);
                    *portf_pm += computed_portf_pm;
                }

                trade_l.extend(new_trade_l);  //*trade_l += &new_trade_l;
                trades_non_pricing.extend(offending_trades);  //*trades_non_pricing += &offending_trades;

                *pns = ProcessorMiddleState::CalculatingSingle;

                let Some(real_market) = market else {
                    warn!("Does not have market. Continuing.");
                    return Ok(());
                };

		self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), real_market.to_string(), myself)
		    )
		)?;
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
		//*trade_l += &new_trade_l;
                trade_l.extend(new_trade_l);

                let Some(real_market) = market else {
                    warn!("Does not have market. Ignoring.");
                    return Ok(());
                };

		self.processor_below.send_message(
		    ProcessorMiddleMessage::NewTradePortfolio(
			(trade_l.clone(), portf.clone(), real_market.to_string(), myself)
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

                let (ref potential_trades, ref potential_portfolio, ref new_market, ref upstream_processor) = ntp;

                info!(
		    "Processor {}, Idle: Switching market <- {}",
		    self.processor_name, new_market
		);


		// switch markets as well
                // self._switch_markets(market, new_market).await?;
                *market = Some(new_market.to_string());

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
                // market is Some, so unwrap is justified.
                let _ = upstream_processor.send_message(
                   ProcessorMiddleMessage::Behind(new_market.to_string(), TradesLocal::new())  // portfolio is accepted, notify the upstream that we're accepting
                );

	    },

	    // TODO: CHECK HERE IF ...MarketSwitch should be handled separately.
	    (ProcessorMiddleMessage::NewTradePortfolio(ntp), ProcessorMiddleState::CalculatingBulk | ProcessorMiddleState::CalculatingBulkMarketSwitch) => {
		// we got new portfolio, but we are in the process of computing the portfolresult of computation has arrived.
		let (potential_trades, potential_portfolio, new_market, upstream_processor) = ntp;

		//if new_market.is_later_than(market) {  // TODO: THIS IS NOT SUFFICIENT CONDITION
		// replace the portfolio, and pass it down

		// TODO: ALSO, SHOULDNT WE NOTIFY THE BULK PROCESSOR THAT WE ARE ABANDONING THE ATTEMPT.
		// TODO: WRONG - IMPLEMENT > JUST FOR REFERENCES!!!

                // let new_behind_curr = potential_trades.clone() - &new_trades;
                let new_behind_curr = trade_l.iter().filter(|x| !potential_trades.contains(x.as_str())).cloned().collect::<TradesLocal>();

		if new_behind_curr.is_empty() {
		    info!(
			"Processor: {}, State: CalculatingBulk: received new portfolio, \
                         was ahead, sending portfolio to processor below",
			self.processor_name,
		    );

                    let Some(real_market) = market else {
                        warn!("Does not have market. Ignoring.");
                        return Ok(());
                    };

		    self.processor_below.send_message(
			ProcessorMiddleMessage::NewTradePortfolio(
			    (trade_l.clone(), portf.clone(), real_market.to_string(), myself)
			)
		    )?;

                    info!(
			"Processor {}, CalculatingBulk|Switch: New portfolio, Switching market.",
			self.processor_name,
		    );


		    // set the state of this processor to the state being sent.
                    //self._switch_markets(market, &_new_market).await?;  // changes markets
                    *market = Some(new_market.clone());  // market switch is simply a name change.
		    *portf = potential_portfolio;
		    *trade_l = potential_trades;
		    *pns = ProcessorMiddleState::CalculatingBulkMarketSwitch;

		} else {
                    info!(
			"Processor {}, CalculatingBulk: current portfolio: {:?}, new portfolio: {:?}. Ignoring the portfolio.",
			self.processor_name, portf.keys(), potential_trades,
                    );

                    // (trade_l, trades_non_pricing, portf, pns, market)
		}
                // send upstream a message that the portfolio is accepted.
                // TODO: CHECK IF THIS SHOULD BE BETTER HANDLED
                // TODO: CHeck if market.unwrap() should be handled.
                let _ = upstream_processor.send_message(
                    ProcessorMiddleMessage::Behind(new_market.to_string(), new_behind_curr)
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

		let (potential_trades, potential_portfolio, new_market, upstream_processor) = ntp;  // new trade portfolio

		// TODO: WRONG - IMPLEMENT > JUST FOR REFERENCES!!!
		//let new_behind_curr = trade_l - &potential_trades;
                let new_behind_curr = trade_l.iter().filter(|&x| !potential_trades.contains(x)).cloned().collect::<TradesLocal>();
                info!(
                    "NewPortfolio: My trades: {}, Potential trades: {}, New trades: {}, New portf: {}",
                    trade_l.len(), potential_trades.len(), new_behind_curr.len(), potential_portfolio.len(),
                );

		if new_behind_curr.is_empty() {
		    // replace the portfolio and trades

		    info!(
			"Processor {}, CalculatingSingle: Portfolio is good, accepting \
			 it and passing to processor below",
			self.processor_name,
		    );
		    *portf = potential_portfolio;
                    trade_l.extend(potential_trades);  //*trade_l += &potential_trades;
		    // TODO: HOW ABOUT pns ???
                    let Some(real_market) = market else {
                        warn!("Does not have market. Ignoring.");
                        return Ok(());
                    };

                    self.processor_below.send_message(
			ProcessorMiddleMessage::NewTradePortfolio(
			    (trade_l.clone(), portf.clone(), real_market.to_string(), myself)
			)
		    )?;

                    info!(
                        "Processor {}, NewPortfolio: switching markets",
                        self.processor_name,
                    );

                    // self._switch_markets(market, &_new_market).await?;
                    *market = Some(new_market.clone());  // TODO: CHECK THIS PART!!!
		}
                // sending upstream that we are done.
                // TODO: CHECK IF THIS SHOULD BE BETTER HANDLED
                // TODO: CHECK IF market.unwrap() should be handled.
                let _ = upstream_processor.send_message(
                    ProcessorMiddleMessage::Behind(new_market.to_string(), new_behind_curr)  // TODO: CHECK IF THIS IS REALLY NEW_MARKET??
                );
	    },

            (ProcessorMiddleMessage::ProcessingStat(_), _) => {}, // processing stat is not for this processor

            // we get new metrics from the metric dispatch
            (ProcessorMiddleMessage::Metric(new_pricing_metrics), _) => {
                todo!()
            },
	}
	Ok(())
    }
}
