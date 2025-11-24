use tracing::{info, warn, debug};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::Arc;
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer;
// use chrono::Local;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;


use crate::portfolio::PortfolioType;
use crate::pricer::{ PricingMetric, PriceTrade};
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_msg::ProcessorMiddleMessage;
use crate::all_markets::AllMarkets;
use crate::market::MarketTypeT;


pub(crate) struct ProcessorCurr<T, MT>
where
    MT: MarketTypeT
{
    pub processor_name: String,
    pub metric: PricingMetric,
    pub results_topic: String,
    pub result_publisher: FutureProducer,
    pub all_markets: Arc<AllMarkets<Arc<MT>>>,  // all_markets is DashMap
    pub all_trades: Arc<TradeRep<T>>,  // all_trades is DashMap
    // trade_processor where we can send the info when the trades are processed
    // pub trade_processor: ActorRef<ProcessorMiddleMessage<dyn MarketTypeT<MP=MP>>>,
}


impl<T, MT> std::fmt::Debug for ProcessorCurr<T, MT>
where
    MT: MarketTypeT
//     dyn MarketTypeT<MP=MP> + Send + Sync: Sized,
//    dyn MarketTypeT<MP=MP>: Sized,
//    T: Send + Sync,
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


#[async_trait]
pub(crate) trait PublishPortfolio
{
    async fn _publish_result_portfolio(
	&self,
	portf: PortfolioType,
    ) -> Result<(), SendError>;
}


#[async_trait]
impl<T, MT> PublishPortfolio for ProcessorCurr<T, MT>
where
    //dyn MarketTypeT<MP=MP> + Send + Sync: Sized,
    // dyn MarketTypeT<MP=MP>: Sized,
    T: Send + Sync,
    MT: Send + Sync + MarketTypeT,
{
    async fn _publish_result_portfolio(
	&self,
	portf: PortfolioType,
    ) -> Result<(), SendError>
    {
	// sends to publisher actor
	let curr_mkt_json = serde_json::ser::to_string(&portf.clone())?;
        let curr_mkt_pv = format!("{{\"{}\": {}}}", self.metric, curr_mkt_json);

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
{
    type Msg = ProcessorMiddleMessage<String>;  // dyn MarketTypeT<MP=MP>>;
    // state is a tuple of
    //    current trades,
    //    current portfolio
    //    current market name
    //       representation.
    type State = (Vec<String>, PortfolioType, String);
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let initial_trades = vec![];
	let initial_curr_portf = PortfolioType::default();
        let market = self.processor_name.clone();

	Ok((initial_trades, initial_curr_portf, market))
    }

    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
	let (trades, portf, market, ) = state;

        match message {
	    ProcessorMiddleMessage::NewTrade(trade) => {

                let Some(trade_info) = self.all_trades.get(&trade) else {
                    warn!("Could not find {} among all_atrades. Ignoring and continuing.", trade);
                    return Ok(());
                };

                info!("Adding new trade: {}", trade);
                let trade_real = trade_info.value();

                let Some(market_info) = self.all_markets.markets.get(market) else {
                    warn!("Could not find market {}. Ignoring the market", market);
                    return Ok(());
                };

                let market_info = market_info.value();
                debug!("Valuing trade {} on market {:?}", trade, market_info.market_name());
		let valued_trade = trade_real.value_by_metric(
		    self.metric,
	            market_info.clone(),
		).await;

		// updating the portfolio
		//*trades += &trade; // TODO: THIS CAN BE FIXED.
                trades.push(trade);
		*portf += valued_trade;

                // send information about all the trades to the trade processor
                // let now = Local::now();
                //self.trade_processor.send_message(
                //    ProcessorMiddleMessage::ProcessingStat(
                //        (self.processor_name.clone(), now.naive_local(), trades.len())
                //    )
                //);

		self._publish_result_portfolio(portf.clone()).await?
            },

	    ProcessorMiddleMessage::NewTradePortfolio((new_trades, new_portfolio, new_market, new_processor)) => {
		// we got a new portfolio, possibly switch it
		// let new_behind_curr = trades.clone() - &new_trades.clone();
                // TODO: This can be better optimized here!!!
                let new_behind_curr = trades.iter().cloned().filter(|x| !new_trades.contains(x)).collect::<Vec<_>>();

		new_processor.send_message(
		    ProcessorMiddleMessage::Behind(new_behind_curr.clone())
		)?;
		info!(
                    "Received new trade portfolio, behind: {:?}, portf size: {}",
                    new_behind_curr.len(),
                    new_portfolio.len(),
                );

		//let send_cnd = new_behind_curr.is_empty();  // new portfolio has more trades.
                if *portf <= new_portfolio {  // when to send the portfolio to publisher.
                //if new_behind_curr.is_empty() {
		    // publish the new portfolio
                    info!("Changing portfolio.");
		    self._publish_result_portfolio(new_portfolio.clone()).await?;

                    // TODO: WHAT TO DO W/ THIS SWITCH_MARKETS
                    // self._switch_markets(market, &new_market).await?;
                    // market that we were holding should be removed from the all_markets,
                    // as it's not needed anymore.
                    // IMPORTANT: this .remove call CAN DEADLOCK!!!
                    let _ = self.all_markets.markets.remove(&market.clone());  // TODO: HANDLE ERROR MESSAGES

		    // update the state of current processor.
		    *portf = new_portfolio;
		    //*trades += &new_trades;
                    // TODO: CHECK IF THIS IS OK
                    trades.extend(new_trades);
		    *market = new_market;
		} // otherwise dont do anything.
	    },

	    _ => {
                panic!("Unusual message. Shouldnt happen");
            },

        }
	Ok(())
    }
}
