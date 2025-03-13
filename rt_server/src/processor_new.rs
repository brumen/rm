use tracing::{info, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;

use crate::market::{AllMarkets, CurrNewMarket, MarketGeneral, MarketSwitching,};
use crate::portfolio::PortfolioType;
use crate::pricer::{Decoder, MarketPricingOptions, PricingMetric, RestPricerSpark};
use crate::process_trade::ProcessTradeValue;
use crate::processor_msg::{ProcessorBulkMessage, ProcessorMiddleMessage,};
use crate::trade::{BaseTrade, TradeRep};
use crate::ao_trade::AOTrade;


#[derive(Debug)]
pub struct ProcessorNew{
    pub metric: PricingMetric,
    pub pricing_options: MarketPricingOptions,
    pub processor_middle: ActorRef<ProcessorMiddleMessage>,  // current processor ref.
    pub processor_bulk: ActorRef<ProcessorBulkMessage>,  // bull processor ref.
    pub r_client: Option<reqwest::Client>,
    pub all_markets: Arc<AllMarkets>,
    pub market_name: CurrNewMarket,
}

#[derive(Debug)]
pub enum ProcessorNewState {
    CalculatingSingle,  // when bulk has finished and we're only calculating single trades.
    CalculatingBulk,  // when we're still calculating bulk
    Idle,
}


impl MarketSwitching for ProcessorNew {

    fn all_markets(&self) -> std::sync::Arc<AllMarkets> {
	self.all_markets.clone()
    }

    fn r_client(&self) ->  &reqwest::Client {
	match &self.r_client {
	    Some(rc) => return &rc,
	    None => panic!("Need client for market switching"),
	}
    }

    fn market_endpoint(&self) -> String {
	format!("http://{0}/market", self.pricing_options.pricing_server.clone())
    }
}


impl Decoder for ProcessorNew {}

impl<ReductionType> RestPricerSpark<ReductionType> for ProcessorNew
where
    ReductionType: PartialEq + Clone + BaseTrade + Sync + Send,
    ProcessorNew: Decoder,
{

    fn _pricing_server_spark(&self) -> String {
	self.pricing_options.pricing_server.clone()
    }

    fn _pricing_endpoint_spark(&self, _market_: CurrNewMarket, _metric: PricingMetric) -> String {
	"/pricing".to_string()
    }
}


#[async_trait]
impl Actor for ProcessorNew {
    type Msg = ProcessorMiddleMessage;
    // first argument is list of trades,
    //   second is the list of trades that didnt price correctly
    //   third is the current portfolio result of correctly pricing trades.
    //   fourth is the computation state.
    type State = (TradeRep<AOTrade>, TradeRep<AOTrade>, PortfolioType, ProcessorNewState);
    type Arguments = ();

    // initialization of the new processor
    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

        Ok(
	    (
		TradeRep::<AOTrade>::default(),
		TradeRep::<AOTrade>::default(),
		PortfolioType::default(),
		ProcessorNewState::Idle,
	    )
	)
    }

    // #[instrument]
    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

	let (trade_l, trades_non_pricing, portf, pns) = state;

	let new_m = CurrNewMarket("new".to_string());

	match message {
	    ProcessorMiddleMessage::NewTrade(new_trade) => {
		//*trade_l += &new_trade;  // we add the trade to the list.

		match pns {  // what is the processor doing right now.
		    ProcessorNewState::CalculatingSingle => {
			// add the trade to the new portfolio and
			//   attempt again.
                        info!(
                            "CalculatingSingle, NewTrade, computing and sending to lower middle.",
                        );
			let new_trade_price = new_trade.value_by_metric2(
			    self.metric,
			    &self.pricing_options,
			    MarketGeneral::MarketRemote(new_m),
			).await;
			*portf += new_trade_price;  // portfolio update
			*trade_l += &new_trade;  // we add the trade to the list.

			// we send the computed portfolio & trades to the current processor
			//   hoping that we are ahead.
			self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), self.market_name.clone(), myself)
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
				(self.market_name.clone(), trade_l.clone(), myself)
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
			    MarketGeneral::MarketRemote(new_m)
			).await;
			*portf += new_trade_price;  // portfolio update
                        self.processor_middle.send_message(
                            ProcessorMiddleMessage::NewTradePortfolio(
                            (trade_l.clone(), portf.clone(), self.market_name.clone(), myself)
                            )
                        )?;
		    },
		}
	    },
	    ProcessorMiddleMessage::NewMarket(new_market) => {
		match pns { // what is the processor doing right now

		    ProcessorNewState::Idle => {
			// update the "new" market
                        info!(
                            "Idle, NewMarket: sending to bulk. State -> Bulk"
                        );
                        info!("Idle, NewMarket: setting new market.");
			self.set_market(
			    new_market,
			    CurrNewMarket("new".to_string()),
			).await?;
			// we are idle, we can start calculating, start calculating
			*pns = ProcessorNewState::CalculatingBulk;
			self.processor_bulk.send_message(
			    ProcessorBulkMessage::NewBulk((
				self.market_name.clone(), trade_l.clone(), myself
			    ))
			)?;
		    },

		    ProcessorNewState::CalculatingSingle => {
			// attempt to send it to processor current
                        info!(
                            "CalculatingSingle, NewMarket: setting Future market."
                        );
			self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), self.market_name.clone(), myself)
			    )
			)?;
			// we're calculating, update the future market, not current
			self.set_market(
			    new_market,
			    CurrNewMarket("future".to_string()
			    )
			).await?;
		    }
		    // ignore if new market comes in, no
		    //   action taken.
		    ProcessorNewState::CalculatingBulk => {
			// just update the future market
                        info!("CalculatingBulk, NewMarket: Setting Future market.");
			self.set_market(
			    new_market,
			    CurrNewMarket("future".to_string()),
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
			    *portf = PortfolioType::default();
			    // this is the exception, as "future" market not in
			    //   the list of markets.
			    let _ = self._switch_markets(
				self.market_name.clone(),
				CurrNewMarket("future".to_string()),
			    ).await;
			    *pns = ProcessorNewState::Idle;

			} else {
			    // we are still behind the current processor.
			    *trade_l += &trades_behind;
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (self.market_name.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk;
			}
		    },

		    // TODO: THIS HAS TO BE REEXAMINED!!!!
		    ProcessorNewState::CalculatingBulk => {
			if trades_behind.is_empty() {
			    // new processor is ahead, reset the
			    //    new processor to the new default state.
			    *portf = PortfolioType::default();
			    *pns = ProcessorNewState::Idle;
			}
			// otherwise we wait for the results of bulk computation.

			// } else {
			//     // new processor is behind, calculate the remaining trades.
			//     // we are still behind the current processor.
			//     self.processor_bulk.send_message(
			// 	ProcessorBulkMessage::NewBulk(
			// 	    (market.clone(), trades_behind.clone(), myself)
			// 	)
			//     )?;
			//     // *trade_l += &trades_behind;
			// }
		    },

		    ProcessorNewState::Idle => {
			if !trades_behind.is_empty() {
			    self.processor_bulk.send_message(
				ProcessorBulkMessage::NewBulk(
				    (self.market_name.clone(), trades_behind.clone(), myself)
				)
			    )?;
			    *pns = ProcessorNewState::CalculatingBulk;
			    //*trade_l += &trades_behind;
			}
		    },
		}
	    },

	    ProcessorMiddleMessage::BulkReceive((new_trade_l, computed_portf, offending_trades, _bulk_market)) => {
		match pns {

		    ProcessorNewState::Idle => {
			info!("Ignoring bulk receive.");
		    },

		    ProcessorNewState::CalculatingBulk => {
			// result of computation has arrived.
			// TODO: FINISH THIS HERE!!!
			*portf += &computed_portf;
			*trade_l += &new_trade_l;
			*trades_non_pricing += &offending_trades;
			self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), self.market_name.clone(), myself)
			    )
			)?;
			*pns = ProcessorNewState::CalculatingSingle;
		    },
		    ProcessorNewState::CalculatingSingle => {
			// result of computation has arrived.
			//  add it to the computation
			// TODO: FINISH THIS HERE!!!
			// panic!("Received BulkReceive while calculating single - Weird");

			*portf = computed_portf;
			*trade_l += &new_trade_l;
			self.processor_middle.send_message(
			    ProcessorMiddleMessage::NewTradePortfolio(
				(trade_l.clone(), portf.clone(), self.market_name.clone(), myself)
			    )
			)?;
		    },
		}
	    },

	    _ => {
		// this type shouldnt occur
		//   TODO: Better error message
		panic!("Message type shouldnt occur");
	    },
	}
	Ok(())
    }
}
