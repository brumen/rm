use tracing::{info, instrument};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use std::sync::{Arc, Mutex};

use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json;
use thiserror;

// use crate::ao_trade::AOTrade;
use crate::market::{AllMarkets, CurrNewMarket};
use crate::portfolio::PortfolioType;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::process_trade::ProcessTradeValue;
use crate::trade::{BaseTrade, TradeRep};
use crate::processor_msg::ProcessorMiddleMessage;
use crate::market::{MarketGeneral, MarketSwitching};


pub(crate) struct ProcessorCurr<T>{
    pub market_name: CurrNewMarket,
    pub metric: PricingMetric,
    pub results_topic: String,
    pub pricing_options: MarketPricingOptions,
    pub result_publisher: FutureProducer,
    pub r_client: Option<reqwest::Client>,  // request client
    pub portf: Arc<Mutex<PortfolioType>>,  // current working portfolio
    pub all_markets: Arc<AllMarkets>,
    pub trades: TradeRep<T>,
}


impl<T> std::fmt::Debug for ProcessorCurr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
	f.write_str("CurrentProcessor({self.market_name})")
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

impl<T> ProcessorCurr<T> {

    /// encodes and sends the portfolio to Kafka client.
    async fn _send_portfolio(
	&self,
	portf: PortfolioType,
    ) -> Result<(), SendError> {
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
	{
	    let mut p = self.portf.lock().unwrap();
	    *p = portf.clone();
	}

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

impl<T> MarketSwitching for ProcessorCurr<T> {

    fn all_markets(&self) -> Arc<AllMarkets> {
	self.all_markets.clone()
    }

    fn r_client(&self) -> &reqwest::Client {
	match &self.r_client {
	    Some(rc) => return &rc,
	    None => panic!("Need client for market switching"),
	}
    }

    fn market_endpoint(&self) -> String {
	format!("http://{0}/market", self.pricing_options.pricing_server.clone())
    }
}


#[async_trait]
impl<T> Actor for ProcessorCurr<T>
where
    T: Sync + Send + 'static + Clone + BaseTrade + std::fmt::Debug + ProcessTradeValue
{
    type Msg = ProcessorMiddleMessage<T>;
    // state is a tuple of current trades,
    //    and current portfolio, and the current market
    //    representation.
    // BELOW IS THE WORKING VERSION:
    //type State = (TradeRep<AOTrade>, PortfolioType, CurrNewMarket);
    type State = (TradeRep<T>, PortfolioType, CurrNewMarket);
    // type State = (TradeRep<impl Clone + for <'a> AddAssign<&'a AOTrade> >, PortfolioType, MarketType);
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let initial_trades = TradeRep::<T>::default();
	let initial_curr_portf = PortfolioType::default();

	Ok((initial_trades, initial_curr_portf, self.market_name.clone()))
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

		info!("Adding new trade: {:?}", trade);
		let valued_trade = trade.value_by_metric2(
		    self.metric, &self.pricing_options,
		    MarketGeneral::MarketRemote(
			self.market_name.clone()
		    ),
		).await;

		// updating the portfolio
		*trades += &trade;
		*portf += valued_trade;

		self._send_portfolio(portf.clone()).await?
            },

	    ProcessorMiddleMessage::<T>::NewTradePortfolio((new_trades, new_portfolio, new_market, new_processor)) => {
		// we got a new portfolio, possibly switch it
		let new_behind_curr = trades.clone() - &new_trades.clone();
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

		    // switch markets on the remote server
		    self.switch_market(self.market_name.clone()).await?;

		    // update the state of current processor.
		    *portf = new_portfolio;
		    *trades += &new_trades;
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
