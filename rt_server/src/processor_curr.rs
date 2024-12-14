use ractor::{ cast, Actor, ActorProcessingErr, ActorRef};

use rdkafka::util::Timeout;
use serde_json::Error;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info, instrument, warn};
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::ao_trade::AOTrade;
use crate::market::{CurrNewMarket, MarketType, TradeMarketDiscovery};
use crate::portfolio::{PortfolioType, PricingResults};
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::{MarketPricingOptions, PricingMetric};
use crate::process_trade::{ObtainMarket, ProcessTradeValue};
use crate::publish::PublishResults;
use crate::trade::{BaseTrade, TradeDirection, TradeReduce, TradeRep};
use crate::processor_new::{ProcessorNew, ProcessorNewMessage};
use crate::streaming::Streaming;
use crate::market::MarketGeneral;


/// ProcessorNew is actor representation of the
///    new processor.
pub struct ProcessorCurr{
    metric: PricingMetric,
    pricing_options: MarketPricingOptions,
    new_processor: ActorRef<ProcessorNewMessage>,
    result_publisher: FutureProducer,
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
    NewTradePortfolio((TradeRep<AOTrade>, PortfolioType)),
}

impl ProcessorCurr {

    /// encodes and sends the portfolio to Kafka client.
    async fn _send_portfolio(
	&self,
	portf: PortfolioType,
	results_topic: String
    ) {
	// sends to publisher actor
	// TODO: ERROR HANDLING HERE
	let curr_mkt_json = serde_json::ser::to_string(&portf.clone()).unwrap();  // TODO: ? 
        let curr_mkt_pv = format!("{{\"{}\": {}}}", self.metric(), curr_mkt_json);

        // implements bytearray(str(dumps(self.curr_market)), ascii))
        let portf_record = FutureRecord::<'_, [u8], [u8]> {
		topic: &results_topic,
		partition: Some(0),
		payload: Some(curr_mkt_pv.as_bytes()),
		key: None, // TODO: pub key: Option<&'a K>,
		timestamp: None,
		headers: None,
        };

	// TODO: ERROR HANDLING HERE!!!
	self.result_publisher.send(
	    portf_record, Timeout::Never
	).await;
    }

}


impl PublishResults for ProcessorCurr {
    fn metric(&self) -> PricingMetric {
	self.metric
    }
}


impl<'a> Actor for ProcessorCurr {
    type Msg = ProcessorCurrMessage;
    // state is a tuple of current trades,
    //    and current portfolio.
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
	let (trades, portf, _curr_market) = state;
	let results_topic = "ao_results".to_string();
	
        match message {
	    ProcessorCurrMessage::NewTrade(trade) => {

		// curr_trade_receiver.recv().await {
		let valued_trade = trade.value_by_metric2(
		    self.metric, &self.pricing_options, MarketGeneral::MarketRemote(CurrNewMarket::New)
		).await;

		// updating the portfolio
		*trades += &trade;
		*portf += valued_trade;

		self._send_portfolio(portf.clone(), results_topic);
            },

	    ProcessorCurrMessage::NewTradePortfolio((new_trades, new_portfolio)) => {
		// we got a new portfolio, possibly switch it
		let new_behind_curr = (new_trades.len() as i32) - (trades.len() as i32);
		if new_behind_curr > 0 {
		    cast!(
			self.new_processor,
			ProcessorNewMessage::Behind(new_behind_curr)
		    );
		} else {
		    // switch the portfolio
		    *portf = new_portfolio;
		    *trades = new_trades;

		    self._send_portfolio(portf.clone(), results_topic);
		    // TODO: IMPLEMENT THIS CORRECTLY
		    // self._switch_all_markets().await; // curr <- new, new <- fut
		}
	    }
        }
	Ok(())
    }
}
