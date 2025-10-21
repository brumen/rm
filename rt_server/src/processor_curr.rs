use tracing::{info, debug, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::{Arc, Mutex};
use rdkafka::error::KafkaError;
use rdkafka::producer::FutureProducer; // , FutureRecord};

use crate::portfolio::PortfolioType;
use crate::pricer::{MarketPricingOptions, PricingMetric};
//use crate::process_trade::ProcessTradeValue;
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_msg::ProcessorMiddleMessage;
use crate::all_markets::AllMarkets;
use crate::market_switching::MarketSwitching;
use crate::market::MarketTypeT;


pub(crate) struct ProcessorCurr<T, MT: MarketTypeT + Clone>{
    pub processor_name: String,
    pub metric: PricingMetric,
    pub results_topic: String,
    pub pricing_options: MarketPricingOptions,
    pub result_publisher: FutureProducer,
    pub r_client: Option<reqwest::Client>,  // request client
    pub portf: Arc<Mutex<PortfolioType>>,  // current working portfolio
    pub all_markets: Arc<AllMarkets<MT>>,  // all_markets is DashMap
    pub all_trades: Arc<TradeRep<T>>,  // all_trades is DashMap
    //    pub curr_trades: Vec<String>,  // current trades that the processor is using
    // trade_processor where we can send the info when the trades are processed
    pub trade_processor: ActorRef<ProcessorMiddleMessage<MT>>,
}


impl<T, MT: MarketTypeT + Clone> std::fmt::Debug for ProcessorCurr<T, MT> {
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


// Simple portfolio sender - it could be anything, not kafka
pub(crate) trait PortfolioSenderSimple {
    async fn _send_portfolio(
	&self,
	portf: PortfolioType,
    ) -> Result<(), SendError>;
}


impl<T, MT: MarketTypeT + Clone> MarketSwitching for ProcessorCurr<T, MT> {

    fn processor_name(&self) -> String {
        self.processor_name.clone()
    }

    fn all_markets(&self) -> Arc<AllMarkets<MT>> {
	self.all_markets.clone()
    }

    fn r_client(&self) -> Option<&reqwest::Client> {
        self.r_client.as_ref()
    }

    fn market_endpoint(&self) -> String {
	format!("http://{0}/market", self.pricing_options.market_server.clone())
    }
}


// T is the representation fo the trade
#[async_trait]
impl<T, MT> Actor for ProcessorCurr<T, MT>
where
    T: Sync + Send + 'static + Clone + BaseTrade + std::fmt::Debug + std::fmt::Display,
    ProcessorCurr<T, MT>: PortfolioSenderSimple,
    MT: MarketTypeT + Clone + Send + 'static  + Sync // TODO: THIS 'static is WRONG
{
    type Msg = ProcessorMiddleMessage<MT>;
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

	let initial_trades = vec![];  // TradeRep::<T>::default();
	let initial_curr_portf = PortfolioType::default();
        let market = self.processor_name.clone();

	Ok((initial_trades, initial_curr_portf, market))
    }

    //#[instrument]
    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
	let (trades, portf, market, ) = state;

        match message {
	    ProcessorMiddleMessage::NewTrade(trade) => {

		info!("Adding new trade: {}", trade);
                // TODO: REWRITE THIS SHIT!!!
                // TODO: WHAT TO DO IF trade_info = None
                let trade_info = self.all_trades.get(&trade).unwrap();


                debug!("Current trade information: {:?}", trade_info);
                // TODO: THIS SHOULD BE REWRITTEN TOO!!!
                let market_info = self.all_markets.get_m(market.to_string()).unwrap();

		let valued_trade = trade_info.value_by_metric2(
		    self.metric,
                    &self.pricing_options,
	            &market_info,
		).await;

		// updating the portfolio
		//*trades += &trade; // TODO: THIS CAN BE FIXED.
                trades.push(trade);
		*portf += valued_trade;

                // send information about all the trades to the trade processor
                self.trade_processor.send_message(
                    ProcessorMiddleMessage::ProcessingStat(
                        (self.processor_name.clone(), chrono::NaiveDateTime::now(), trades.len())
                    )
                );

		self._send_portfolio(portf.clone()).await?
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
		    self._send_portfolio(new_portfolio.clone()).await?;

                    // TODO: WHAT TO DO W/ THIS SWITCH_MARKETS
                    self._switch_markets(market, &new_market).await?;

		    // update the state of current processor.
		    *portf = new_portfolio;
		    //*trades += &new_trades;
                    // TODO: CHECK IF THIS IS OK
                    trades.extend(new_trades);

		    // *market = new_market;

		} // otherwise dont do anything.
	    },

	    _ => {
                panic!("Unusual message. Shouldnt happen");
            },

        }
	Ok(())
    }
}
