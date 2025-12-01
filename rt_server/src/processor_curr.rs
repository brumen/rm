use tracing::{info, warn, debug};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
// use chrono::Local;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use std::collections::HashMap;

use crate::portfolio::{PortfolioType, PmPortfolio};
use crate::pricer::{ PricingMetric, PriceTrade};
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_msg::{ProcessorMiddleMessage, TradesLocal};
use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;


pub(crate) struct ProcessorCurr<T, MT>
where
    MT: MarketTypeT
{
    pub processor_name: String,
    pub results_topic: String,
    pub result_publisher: FutureProducer,
    pub all_markets: Arc<AllMarkets<Arc<MT>>>,  // all_markets is DashMap
    pub all_trades: Arc<TradeRep<T>>,  // all_trades is DashMap
    // trade_processor where we can send the info when the trades are processed
    // pub trade_processor: ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>>,
}


impl<T, MT> ProcessorCurr<T, MT>
where
    MT: MarketTypeT,
{
    /// changes the PmPortfolio with respect to the new metrics that it receives.
    fn _change_metrics(present_pm: PmPortfolio, new_metrics: Vec<PricingMetric>) -> PmPortfolio {
        todo!()
    }

}


impl<T, MT> std::fmt::Debug for ProcessorCurr<T, MT>
where
    MT: MarketTypeT
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CurrentProcessor({self.processor_name})")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SendError {
    #[error("Cant send to kafka")]
    // KafkaError(#[from] KafkaError),
    KafkaErr(KafkaError),
    #[error("Cant serialize")]
    SerializeError(#[from] serde_json::Error),
}


/// publish the portfolio for a particular metric
#[async_trait]
pub(crate) trait PublishPortfolio
{
    async fn _publish_result_portfolio(
	&self,
	portf: PortfolioType,
        metric: PricingMetric,
    ) -> Result<(), SendError>;
}


#[async_trait]
impl<T, MT> PublishPortfolio for ProcessorCurr<T, MT>
where
    T: Send + Sync,
    MT: Send + Sync + MarketTypeT,
{
    async fn _publish_result_portfolio(
	&self,
	portf: PortfolioType,
        metric: PricingMetric,
    ) -> Result<(), SendError>
    {

	// sends to publisher actor
	let curr_mkt_json = serde_json::ser::to_string(&portf.clone())?;
        let curr_mkt_pv = format!("{{\"{}\": {}}}", metric, curr_mkt_json);

        // implements bytearray(str(dumps(self.curr_market)), ascii))
        let portf_record = FutureRecord::<'_, [u8], [u8]> {
		topic: &self.results_topic,
		partition: Some(0),
		payload: Some(curr_mkt_pv.as_bytes()),
		key: None, // TODO: pub key: Option<&'a K>,
		timestamp: None,
		headers: None,
        };

	// set up the portfolio in self
	//{
	//    let mut p = self.portf.lock().unwrap();
	//    *p = portf.clone();
	//}

	// first i32 = partition
	// second i64 = offset
	// error is the Kafka error
	// OwnedMessage - copy of the original message.
	// Result<(i32, i64), (KafkaError, OwnedMessage)>;
	match self.result_publisher.send(portf_record, Timeout::Never).await {
	    Err((ke, _)) => Err(SendError::KafkaErr(ke)),
	    _ => Ok(()),
	}
    }
}


// T is the representation fo the trade
#[async_trait]
impl<T, MT> Actor for ProcessorCurr<T, MT>
where
    T: Sync + Send + Clone + BaseTrade + PriceTrade<MT> + 'static,
    ProcessorCurr<T,MT>: PublishPortfolio,
    MT: MarketTypeT + Send + Sync + 'static,
    MT::MP: Clone,
{
    type Msg = ProcessorMiddleMessage<String>;  // dyn MarketTypeT<MP=MP>>;
    // state is a tuple of
    //  1st arg:  current trades,
    //  2nd arg:  a map of metrics to portfolioTypes, e.g. PV: Portf1, PV01: Portf2...
    //  3rd arg:  current market name
    //  4th arg:  vector of pricing metrics for which we are computing.
    type State = (TradesLocal, PmPortfolio, Option<String>, Vec<PricingMetric>);
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let initial_trades = TradesLocal::new();
	let initial_curr_portf = PmPortfolio::new();
        // no initial metrics
	Ok(
            (initial_trades, initial_curr_portf, None, vec![])
        )
    }

    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
	let (trades, portf, market, curr_pricing_metrics) = state;

        match message {
	    ProcessorMiddleMessage::NewTrade(trade) => {

                let Some(real_market) = market else {
                    // only continue if you have a market.
                    warn!("Do not have market. Ignoring the trade.");
                    return Ok(());
                };

                let Some(trade_info) = self.all_trades.get(&trade) else {
                    warn!("Could not find {} among all_atrades. Ignoring and continuing.", trade);
                    return Ok(());
                };

                info!("Adding new trade: {}", trade);
                let trade_real = trade_info.value();

                let Some(market_info) = self.all_markets.get(real_market) else {
                    warn!("Could not find market {}. Ignoring the market", real_market);
                    return Ok(());
                };

                debug!(
                    "Valuing trade {} on market {:?} for {:?}",
                    trade, market_info.market_name(), curr_pricing_metrics
                );
                for pm in curr_pricing_metrics {
		    let valued_trade_pm = trade_real.value_by_metric(
		        *pm,
	                market_info.clone(),
		    ).await;
                    let portf_pm = portf.get(*pm);
                    *portf_pm += valued_trade_pm;
                }

		// updating the portfolio
		//*trades += &trade; // TODO: THIS CAN BE FIXED.
                trades.insert(trade);
		// *portf += valued_trade;
                debug!(
                    "Current market: {:?}, portfolio: {:?}", market, portf
                );
                // send information about all the trades to the trade processor
                // let now = Local::now();
                //self.trade_processor.send_message(
                //    ProcessorMiddleMessage::ProcessingStat(
                //        (self.processor_name.clone(), now.naive_local(), trades.len())
                //    )
                //);

                for pm in curr_pricing_metrics {
                    let portf_pm = portf.get(*pm);
		    self._publish_result_portfolio(portf_pm.clone(), *pm).await?
                }
            },

	    ProcessorMiddleMessage::NewTradePortfolio((new_trades, new_portfolio, new_market, upstream_processor)) => {
		// we got a new portfolio, possibly switch it

		// let new_behind_curr = trades - new_trades;
                let new_behind_curr = trades.iter().filter(|&x| !new_trades.contains(x.as_str())).cloned().collect::<TradesLocal>();

		info!(
                    "Received new trade portfolio, behind: {:?}, portf size: {}",
                    new_behind_curr.len(),
                    new_portfolio.len(),
                );

		// new portfolio has more trades, send the portfolio to publisher.
                if *portf <= new_portfolio {
		    self._publish_result_portfolio(new_portfolio.clone()).await?;

                    // market that we were holding should be removed from the all_markets,
                    // as it's not needed anymore.
                    // IMPORTANT: this .remove call CAN DEADLOCK!!!
                    // destroys the market at the end.
                    if let Some(real_market) = market {
                        if *real_market != new_market {  // only destroy if the markets are different
                            info!("Destroying the market {}", real_market);
                            let _ = self.all_markets.remove(&real_market.clone());  // TODO: HANDLE ERROR MESSAGES
                        }
                    };

		    // update the state of current processor.
		    *portf = new_portfolio;
                    trades.extend(new_trades);  // *trades += &new_trades;
		    *market = Some(new_market.clone());  // markets should trickle down.
                    debug!("Switching to market {:?}", market);  // market should be created.
                    info!("Market: {:?}, Portfolio: {:?}", market, portf);
		} // otherwise dont do anything.

                // send the behind information to the middle processor.
                upstream_processor.send_message(
		    ProcessorMiddleMessage::Behind(new_market, new_behind_curr.clone())
		)?;

	    },

            // we get a portfolio of different metrics
            ProcessorMiddleMessage::Metric(new_pricing_metrics) => {
                // TODO: CHECK THE CONDITIONS, SO THAT YOU DONT HAVE TO COPY
                *portf = Self::_change_metrics(*portf, new_pricing_metrics);

                // sending it to for publishing
                for (pm, portf_pm) in portf.iter() {
                    self._publish_result_portfolio(portf_pm.clone(), *pm).await?;
                }
            }

	    _ => {
                panic!("Unusual message. Shouldnt happen");
            },

        }
	Ok(())
    }
}
