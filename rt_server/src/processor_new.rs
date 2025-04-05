use tracing::{info, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;

use crate::market::{MarketType, MarketSwitching, AllMarkets, };
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage,};
use crate::trade::{BaseTrade, TradeRep};


#[derive(Debug)]
pub struct ProcessorNew<T>{
    pub processor_name: String,
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub processor_middle: ActorRef<ProcessorMiddleMessage<T>>,  // current processor ref.
    pub processor_bulk: ActorRef<ProcessorBulkMessage<T>>,  // bull processor ref.
    pub r_client: Option<reqwest::Client>,
    pub all_markets: Arc<AllMarkets>,
    pub market_name: (MarketType, MarketType),  // first item: new market, second item: future market.
}

#[derive(Debug)]
pub enum ProcessorNewState {
    CalculatingSingle,  // when bulk has finished and we're only calculating single trades.
    CalculatingBulk,  // when we're still calculating bulk
    Idle,
}


impl<T> ProcessorNew<T> {

    /// replaces the future_mkt with replace_mkt.
    ///   either on the server or in the controller.
    async fn _replace_fut_market(
        &self,
        replace_mkt: MarketType,
        future_mkt: &mut MarketType
    ) -> Result<(), ActorProcessingErr> {
        let actual_market = replace_mkt.market;
        match self.r_client() {
            Some(_) => {
		self.set_market(
		    MarketType{
                        market_name: "future".to_string(),
                        market: actual_market
                    },
		    future_mkt,
		).await?;
            },
            None => {
                *future_mkt = MarketType{
                    market_name: "future".to_string(),
                    market: actual_market,
                };
            }
        }
        Ok(())
    }
}


impl<T> MarketSwitching for ProcessorNew<T> {

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


impl<T> Decoder for ProcessorNew<T> {}

impl<ReductionType, T> RestPricerSpark<ReductionType> for ProcessorNew<T>
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorNew<T>: Decoder,
{

    fn _pricing_server_spark(&self) -> String {
	self.pricing_options.pricing_server.clone()
    }

    fn _pricing_endpoint_spark(&self, _market_: MarketType, _metric: PricingMetric) -> String {
	"/pricing".to_string()
    }
}


#[async_trait]
impl<T> Actor for ProcessorNew<T>
where
    T: Send + Clone + 'static + BaseTrade + ProcessTradeValue
{
    type Msg = ProcessorMiddleMessage<T>;
    // first argument is list of trades,
    //   second is the list of trades that didnt price correctly
    //   third is the current portfolio result of correctly pricing trades.
    //   fourth is the computation state.
    //   fifth is the tuple: (new market where we are pricing now, future_market)
    type State = (TradeRep<T>, TradeRep<T>, PortfolioType, ProcessorNewState, (MarketType, MarketType));
    type Arguments = ();

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

        info!("Starting processor.");

        Ok(
	    (
		TradeRep::<T>::default(),
		TradeRep::<T>::default(),
		PortfolioType::default(),
		ProcessorNewState::Idle,
                (
                    MarketType::new(self.processor_name.clone()),
                    MarketType::new("future".to_string())
                ),
	    )
	)
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	let (trade_l, trades_non_pricing, portf, pns, (ref mut new_m, future_m)) = state;

        info!(
            "State: {:?}. Portf size: {}, Nb trades: {}",
            pns, portf.len(), trade_l.len()
        );

	match message {
	    ProcessorMiddleMessage::NewTrade(new_trade) => {
		//*trade_l += &new_trade;  // we add the trade to the list.

		match pns {  // what is the processor doing right now.
		    ProcessorNewState::CalculatingSingle => {
			// add the trade to the new portfolio and
			//   attempt again.
                        info!(
                            "CalculatingSingle, NewTrade:, computing trade.",
                        );
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric,
			    &self.pricing_options,
			    new_m.clone(),  // TODO: FIX THIS HERE!!
			).await;
			*portf += new_trade_price;  // portfolio update
			*trade_l += &new_trade;  // we add the trade to the list.

			// we send the computed portfolio & trades to the current processor
			//   hoping that we are ahead.
                        info!("CalculatingSingle, NewTrade: Sending to middle processor");
			self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), new_m.clone(), myself)
			    )
			)?;
		    },

		    ProcessorNewState::Idle => {
			// start the new portfolio construction.
                        info!(
                            "Idle, NewTrade: Sending all {} trades to bulk. Going -> CalculatingBulk.",
                            trade_l.len(),
                        );
			*trade_l += &new_trade;
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk(
				(new_m.clone(), trade_l.clone(), myself)
			    )
			)?;
			*pns = ProcessorNewState::CalculatingBulk;
		    },

		    ProcessorNewState::CalculatingBulk => {
                        info!(
                            "CalculatingBulk, NewTrade: adding trade and sending to lower."
                        );

			*trade_l += &new_trade;  // we add the trade to the list.
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric,
			    &self.pricing_options,
			    new_m.clone(),  // TODO: CHECK IF THIS IS NECESSARY HERE!!!
			).await;
			*portf += new_trade_price;  // portfolio update
                        info!(
                            "CalculatingBulk, NewTrade: Sending to lower processor. Portf size: {}",
                            portf.len(),
                        );
                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio(
                            (trade_l.clone(), portf.clone(), new_m.clone(), myself)
                            )
                        )?;
		    },
		}
	    },
	    ProcessorMiddleMessage::NewMarket(new_market) => {

                match pns { // what is the processor doing right now

		    ProcessorNewState::Idle => {
			// update the "new" market
                        info!("Idle, NewMarket: setting new market.");

                        self._replace_fut_market(
                            new_market,  //replace_mkt: MarketType,
                            future_m,  // future_mkt: &mut MarketType
                        ).await?;

			// we are idle, we can start calculating, start calculating
                        info!("Idle, NewMarket: sending to bulk. State -> CalculatingBulk");
                        *pns = ProcessorNewState::CalculatingBulk;
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk((
				new_m.clone(), trade_l.clone(), myself
			    ))
			)?;
		    },

		    ProcessorNewState::CalculatingSingle => {
			// attempt to send it to processor current
                        info!(
                            "CalculatingSingle, NewMarket: sending portfolio \
                             to lower processor, portf size: {}",
                            portf.len(),
                        );
			self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), new_m.clone(), myself)
			    )
			)?;
			// we're calculating, update the future market, not current
                        info!(
                            "CalculatingSingle, NewMarket: setting Future market."
                        );

                        self._replace_fut_market(
                            new_market,  //replace_mkt: MarketType,
                            future_m,  // future_mkt: &mut MarketType
                        ).await?;
		    }
		    // ignore if new market comes in, no
		    //   action taken.
		    ProcessorNewState::CalculatingBulk => {
			// just update the future market
                        info!("CalculatingBulk, NewMarket: Setting Future market.");

                        self._replace_fut_market(
                            new_market,  //replace_mkt: MarketType,
                            future_m,  // future_mkt: &mut MarketType
                        ).await?;
		    },
		}
	    },

	    // this only comes from ProcessorBulk, so we already launched a bulk request.
	    ProcessorMiddleMessage::Behind(trades_behind) => {
		// we are behind trades behind the current processor
		match pns { // what is the processor doing right now
		    ProcessorNewState::CalculatingSingle => {
			// TODO: HERE PERHAPS CONSIDER DEPENDING ON HOW MANY
			// TRADES ARE BEHIND,
			//    < 10 -> continue in single mode
			//    > 10 -> continue in bulk mode.
			if trades_behind.is_empty() {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
                            info!(
                                "Behind, CalculatingSingle: Successfully accepted. Resetting portfolio."
                            );
                            *portf = PortfolioType::default();
			    // this is the exception, as "future" market not in
			    //   the list of markets.
                            info!(
                                "Behind, CalculatingSingle: Switching markets: {} <- future",
                                new_m.clone(),
                            );

                            match self.r_client() {
                                Some(_) => {
                                    self._switch_markets(
				        future_m,
				        &new_m,
			            ).await?;
                                },
                                None => {
                                    *new_m = future_m.clone();  // TODO: CHECK IF THIS IS RIGHT????
                                }
                            }
                            info!("CalculatingSingle, Behind: Going to state Idle.");
                            *pns = ProcessorNewState::Idle;

			} else {
			    // we are still behind the current processor.
                            info!(
                                "Behind, CalculatingSingle: Still behind lower processor,\
                                 adding trades ({}) and computing bulk.",
                                trade_l.len(),
                            );
			    *trade_l += &trades_behind;
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (new_m.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk;
			}
		    },

		    // we are calculating bulk, and we received info
                    //   from processor below.
		    ProcessorNewState::CalculatingBulk => {
                        // add the trades to portfolio, nothing else.
                        if !trades_behind.is_empty() {
                            info!(
                                "CalculatingBulk, Behind: adding non-computed trades to trade list."
                            );
                            *trade_l += &trades_behind;

			}
		    },

		    ProcessorNewState::Idle => {
			if !trades_behind.is_empty() {
                            info!(
                                "Idle, Behind: Starting new bulk compute."
                            );
                            *trade_l += &trades_behind;
                            self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (new_m.clone(), trade_l.clone(), myself)
				)
			    )?;
                            info!(
                                "Idle, Behind: Going into state -> CalculatingBulk",
                            );
			    *pns = ProcessorNewState::CalculatingBulk;
			}
		    },
		}
	    },

	    ProcessorMiddleMessage::BulkReceive((new_trade_l, computed_portf, offending_trades, _bulk_market)) => {
		match pns {

		    ProcessorNewState::Idle => {
			info!("Idle: Ignoring bulk receive.");  // TODO: CHECK THIS PART
		    },

		    ProcessorNewState::CalculatingBulk => {
			// result of computation has arrived.
			// TODO: FINISH THIS HERE!!!
			*portf += &computed_portf;
			*trade_l += &new_trade_l;
			*trades_non_pricing += &offending_trades;
                        info!(
                            "CalculatingBulk, BulkReceive: sending to lower processor, portf size: {}",
                            portf.len(),
                        );
                        self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), new_m.clone(), myself)
			    )
			)?;
                        info!(
                            "CalculatingBulk, BulkReceive: Going to Single computation mode",
                        );
                        *pns = ProcessorNewState::CalculatingSingle;
		    },
		    ProcessorNewState::CalculatingSingle => {
			// result of computation has arrived.
			//  add it to the computation
			// TODO: FINISH THIS HERE!!!
			// panic!("Received BulkReceive while calculating single - Weird");

			*portf = computed_portf;
			*trade_l += &new_trade_l;
                        info!(
                            "CalculatingSingle, BulkReceive: sending to lower processor. Portf size: {}",
                            portf.len(),
                        );
			self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), new_m.clone(), myself)
			    )
			)?;
		    },
		}
	    },

	    _ => {
		// this type shouldnt occur
		//   TODO: Better error message
		panic!("This Message type shouldnt occur.");
	    },
	}
	Ok(())
    }
}
