use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};

use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::ao_trade::AOTrade;
use crate::market::{CurrNewMarket, MarketType};
use crate::portfolio::PortfolioType;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::process_trade::ProcessTradeValue;
use crate::trade::TradeRep;
use crate::processor_new::ProcessorNewMessage;
use crate::streaming::Streaming;
use crate::market::MarketGeneral;


pub struct ProcessorCurr{
    pub metric: PricingMetric,
    pub results_topic: String,
    pub pricing_options: MarketPricingOptions,
    pub result_publisher: FutureProducer,
}

impl Streaming for ProcessorCurr {
    fn kafka_server_name(&self) -> String {
	self.pricing_options.pricing_server.clone()  // TODO: CHECK THE CLONGING HERE!!!
    }

    fn kafka_port(&self) -> i32 {
	5000  // TODO: THIS IS WRONG
    }
}

pub enum ProcessorCurrMessage {
    NewTrade(AOTrade),
    NewTradePortfolio((TradeRep<AOTrade>, PortfolioType, MarketType, ActorRef<ProcessorNewMessage>)),
}

impl ProcessorCurr {

    /// encodes and sends the portfolio to Kafka client.
    async fn _send_portfolio(
	&self,
	portf: PortfolioType,
    ) -> Result<(), KafkaError> {
	// sends to publisher actor
	// TODO: ERROR HANDLING HERE
	let curr_mkt_json = serde_json::ser::to_string(&portf.clone()).unwrap();
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

	// first i32 = partition
	// second i64 = offset
	// error is the Kafka error
	// OwnedMessage - copy of the original message. 
	// Result<(i32, i64), (KafkaError, OwnedMessage)>;
	match self.result_publisher.send(portf_record, Timeout::Never).await {
	    Err((ke, _)) => {
		Err(ke)
	    },
	    _ => {Ok(())},
	}
    }
}


#[async_trait]
impl Actor for ProcessorCurr {
    type Msg = ProcessorCurrMessage;
    // state is a tuple of current trades,
    //    and current portfolio, and the current market
    //    representation.
    type State = (TradeRep<AOTrade>, PortfolioType, MarketType);  
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let initial_trades = TradeRep::<AOTrade>::default();
	let initial_curr_portf = PortfolioType::default();
	let initial_market = MarketType::new();
	
	Ok((initial_trades, initial_curr_portf, initial_market))
    }

    async fn handle(
        &self,
	_myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
	let (trades, portf, market, _curr_market) = state;
	
        match message {
	    ProcessorCurrMessage::NewTrade(trade) => {

		let valued_trade = trade.value_by_metric2(
		    self.metric, &self.pricing_options,
		    MarketGeneral::MarketRemote(CurrNewMarket::Current)
		).await;

		// updating the portfolio
		*trades += &trade;
		*portf += valued_trade;

		self._send_portfolio(portf.clone()).await?
            },

	    ProcessorCurrMessage::NewTradePortfolio((new_trades, new_portfolio, new_market, new_processor)) => {
		// we got a new portfolio, possibly switch it
		let new_behind_curr = (new_trades.len() as i32) - (trades.len() as i32);
		new_processor.send_message(
		    ProcessorNewMessage::Behind(new_behind_curr)
		)?;

		if new_behind_curr < 0 {
		    // switch the portfolio
		    *portf = new_portfolio;
		    *trades = new_trades;
		    *market = new_market;

		    // publish the new portfolio
		    self._send_portfolio(portf.clone()).await?
		    // TODO: WHERE DOES THE MARKET SWITCH??? HERE??? 
		    // self._switch_all_markets().await; // curr <- new, new <- fut
		}
	    }
        }
	Ok(())
    }
}
