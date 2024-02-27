use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{info, warn, instrument, error};
use core::convert::From;
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use crate::ao_trade::{AOTrade, AOTradeRep};
use crate::market::{
    CurrNewMarket, MarketSwitching, MarketType, MktMsgParams, TradeMarketDiscovery, MarketGeneral,
};
use crate::mkt_handler::MktEventHandler;
use crate::portfolio::PortfolioType;
use crate::pricer::{
    Decoder, MarketPricingOptions, PricingMetric, PricingStruct, RestPricerSpark,
};
use crate::publish::PublishResults;
use crate::streaming::Streaming;
use crate::trade::{BaseTrade, TradeReduce, TradeRep};
use crate::trade_procs::RiskProcessors;
use crate::process_trade::ObtainMarket;

pub type PricingParams = HashMap<String, f64>;

/// Controller structure.
/// pricing_params: parameters pushed to the pricing server
/// kafka_server_name: name of kafka server, like "localhost"
/// kafka_port: port of kafka server, like 9092
/// trader_pricer: name of the rester service, like localhost:5010
#[derive(Debug)]
pub struct Controller {
    pricing_params: PricingParams,
    kafka_server_name: String,
    kafka_port: i32,
    trade_pricer: String,
    metric: PricingMetric,
    curr_mkt: Arc<Mutex<MarketType>>,
    new_mkt: Arc<Mutex<MarketType>>,
    future_mkt: Arc<Mutex<MarketType>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RTConfig {
    pub kafka_server_name: String,
    pub kafka_server_port: i32,
    pub mkt_topic: String,
    pub results_topic: String,
    pub pos_topic: String,
    pub trade_pricer: String,
    pub pricing_params: PricingStruct,
    pub metric: String,
    pub pricing_server: String,
}

impl Streaming for Controller {
    fn kafka_server_name(&self) -> String {
        self.kafka_server_name.clone() // TODO: CHECK IF THIS CAN BE REMOVED HERE!!!
    }

    fn kafka_port(&self) -> i32 {
        self.kafka_port
    }
}

impl PublishResults for Controller {
    fn metric(&self) -> PricingMetric {
        self.metric
    }
}

impl MktEventHandler for Controller {

    /// _handle_mkt_msg - for controller
    ///    the message is sent to update future_market.
    ///    future_market becomes new_market when the trade
    ///    processor determines it should be switched.
    async fn _handle_mkt_msg(
        &self,
        market_obj: MarketType,
        new_mkt_sender: Sender<MarketType>,
        _mkt_params: MktMsgParams,
    ) {
	    **(self._future_mkt().lock().expect("Could not lock self.future_mkt")) = market_obj.clone();  // TODO: IS THIS CLONE NECESSARY???
        if let Err(e) = new_mkt_sender.send(market_obj).await {
            warn!("Could not send a message to the new market: {:?}", e);
        }
    }
}

impl ObtainMarket for Controller {
    fn get_market(
        &self,
        curr_new_mkt: CurrNewMarket,
    ) -> MarketGeneral {
        MarketGeneral::MarketRemote(curr_new_mkt)
    }
}

impl MarketSwitching for Controller {
    /// switch markets on the trade api.
    async fn _switch_all_markets(&self) {
        info!("_switch_markets: Switching markets: current <- new.");
	    self._internal_switch_all_markets();  // curr <- new, new <- future
	    // update the markets on the server.

	    let client = reqwest::Client::new();  // async client

	    let market_post = client
            .post(format!("http://{0}/market", self.trade_pricer))
            .json(&HashMap::from([("market", &*self.curr_mkt.lock().unwrap())]))
            .send();

	    let new_market_post = client
            .post(format!("http://{0}/new_market", self.trade_pricer))
            .json(&HashMap::from([("market", &*self.new_mkt.lock().unwrap())]))
            .send();

	    let future_market_post = client
            .post(format!("http://{0}/future_market", self.trade_pricer))
            .json(&HashMap::from([("market", &*self.future_mkt.lock().unwrap())]))
            .send();

	    let (curr_market_post_res, new_market_post_res, future_mkt_post_res) = tokio::join!(
	        market_post,
	        new_market_post,
	        future_market_post,
	    );

        // handling potential errors
        for (mkt_name, mkt_result) in vec![
            ("CURRENT", curr_market_post_res),
            ("NEW", new_market_post_res),
            ("FUTURE", future_mkt_post_res)
        ] {
            match mkt_result {
                Ok(_) => { },
                Err(mpe) => {
                    error!("Error posting to {:?} market: {:?}", mkt_name, mpe);
                }
            }
        }
    }

    /// replaces new market w/ future market.
    ///    and updates them on the server.
    async fn _switch_new_fut_markets(&self) {
	    self._internal_switch_new_fut_markets();

	    let client = reqwest::Client::new();

	    let new_market_post = client
            .post(format!("http://{0}/new_market", self.trade_pricer))
            .json(&HashMap::from([("market", &*self.new_mkt.lock().unwrap())]))
            .send();

	    let future_market_post = client
            .post(format!("http://{0}/future_market", self.trade_pricer))
            .json(&HashMap::from([("market", &*self.future_mkt.lock().unwrap())]))
            .send();

	    let (new_mkt_res, fut_mkt_res) =
	        tokio::join!(
	            new_market_post,
	            future_market_post,
	        );
        for (mkt_name, mkt_result) in vec![
            ("NEW", new_mkt_res),
            ("FUTURE", fut_mkt_res)
        ] {
            match mkt_result {
                Ok(_) => { },
                Err(mpe) => {
                    error!("Error posting to {:?} market: {:?}", mkt_name, mpe);
                }
            }
        }
    }

    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>> {
        self.curr_mkt.clone()
    }

    fn _new_mkt(&self) -> Arc<Mutex<MarketType>> {
        self.new_mkt.clone()
    }

    fn _future_mkt(&self) -> Arc<Mutex<MarketType>> {
	    self.future_mkt.clone()
    }

    /// whether the future market is "ready", i.e.
    /// was there anything new added to the market to
    /// be different from new_mkt
    fn _future_mkt_ready(&self) -> bool {

	    let fut_mkt_ready = *self.new_mkt.lock().unwrap() != *self.future_mkt.lock().unwrap();
        info!("_future_mkt_ready: {:?}", fut_mkt_ready);
        fut_mkt_ready
    }
}


impl TradeMarketDiscovery for Controller { }


// TradeReduce reduces the trade to empty,
// we dont need any additional information from the trade.
impl TradeReduce for Controller {
    type ReductionType = AOTradeRep;
    type TradeType = AOTrade;

    fn reduce(&self, trade: &Self::TradeType) -> Self::ReductionType {
        AOTradeRep(trade.id())
    }
}


impl RiskProcessors for Controller {

    // prices trades provided in all_trades for metric, and selected market.
    //#[instrument]
    async fn _price_existing_trades(
        &self,
        all_trades: &TradeRep<Self::ReductionType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType {
        return self.price_trades_on_spark(
            all_trades,
            metric,
            pricing_options,
            curr_new_mkt
        ).await;
    }

    async fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &mut Receiver<Self::ReductionType>,
        all_trades: &mut TradeRep<Self::ReductionType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<Self::ReductionType>)>,
    ) {
        self._price_new_trades_seq(
            curr_portfolio,
            trade_receiver,
            all_trades,
            metric,
            pricing_options,
            curr_new_mkt,
            new_trades_sender,
        ).await
    }
}

// implementation of decoder for results on the portfolio.
impl Decoder for Controller {}

impl RestPricerSpark<AOTradeRep> for Controller {
    fn _pricing_server_spark(&self) -> String {
        self.trade_pricer.to_owned() // TODO: THIS IS GARBAGE
    }

    fn _pricing_endpoint_spark(&self, market_: CurrNewMarket, metric: PricingMetric) -> String {
        let metric_str = match metric {
            PricingMetric::PV => "pv".to_owned(),
            PricingMetric::PV01 => "pv01".to_owned(),
            _ => todo!(),
        };

        let market_str = match market_ {
            CurrNewMarket::Current => "spark".to_owned(),
            CurrNewMarket::New => "spark_new".to_owned(),
        };

        format!("{}/{}", metric_str, market_str)
    }
}

// Controller is generic over MarketType type, which originally was (String, Date)
impl Controller {

    pub fn new(
        pricing_params_: Option<PricingParams>,
        kafka_server_name: String,
        kafka_port: i32,
        trade_pricer: String,
        metric: PricingMetric,
    ) -> Self {
        // if given the params, use them, otherwise construct empty map
        let pricing_init = match pricing_params_ {
            Some(pricing_hash) => pricing_hash,
            None => PricingParams::new(),
        };

        // let (num_tokio_worker_threads, max_tokio_blocking_threads) = (8, 512); // 512 is tokio's current default
        // let rt = runtime::Builder::new_multi_thread()
        //     .enable_all()
        //     .thread_stack_size(8 * 1024 * 1024)
        //     .worker_threads(num_tokio_worker_threads)
        //     .max_blocking_threads(max_tokio_blocking_threads)
        //     .build()
        //     .unwrap();

        // let mkt_client_address = "http://localhost:8000/future_market";
        // let client = reqwest::blocking::Client::new();  // TODO: THIS ALWAYS REPEATS!!!
        // let rb = client.post(format!("{0}", mkt_client_address));

        Controller {
            pricing_params: pricing_init,
            kafka_server_name,
            kafka_port,
            trade_pricer,
            metric,
            curr_mkt: Arc::new(Mutex::new(MarketType::new())),
            new_mkt: Arc::new(Mutex::new(MarketType::new())),
	    future_mkt: Arc::new(Mutex::new(MarketType::new())),
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(config_file: String) -> Result<Self, Box<dyn std::error::Error>> {
        let config_f = std::fs::File::open(config_file)?;
        let config_map: RTConfig = serde_yaml::from_reader(config_f)?;

        let controller_metric = if config_map.metric == *"PV" {
            PricingMetric::PV
        } else {
            PricingMetric::PV01
        };

        Ok(Controller::new(
            // pricing_params
            Some(HashMap::from([
                ("nb_sim".to_owned(), config_map.pricing_params.nb_sim as f64),
                (
                    "default_price".to_string(),
                    config_map.pricing_params.default_price,
                ),
            ])),
            config_map.kafka_server_name,
            config_map.kafka_server_port,
            config_map.trade_pricer,
            controller_metric,
        ))
    }

    #[instrument]
    async fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &mut Receiver<AOTradeRep>,
        all_trades: &mut TradeRep<AOTradeRep>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<AOTradeRep>)>,
    ) {
	    self._price_new_trades_seq(
	        curr_portfolio,
	        trade_receiver,
	        all_trades,
	        metric,
	        pricing_options,
	        curr_new_mkt,
	        new_trades_sender,
	    ).await;
    }

    /// prices trades sequentially.
    #[instrument]
    async fn _price_new_trades_seq(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &mut Receiver<AOTradeRep>,
        all_trades: &mut TradeRep<AOTradeRep>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<AOTradeRep>)>,
    ) {

        while let Ok(trade) = trade_receiver.try_recv() {
            *all_trades += &trade;
            *curr_portfolio += self
                ._process_trade(&trade, metric, pricing_options, curr_new_mkt)
                .await;
	        let new_p_attempt =  new_trades_sender.send(
		        (curr_portfolio.clone(), TradeRep(all_trades.clone()))
	        ).await;

	        match new_p_attempt {
		        Ok(_) => {},
		        Err(e) => {
		            warn!("Could not send portfolio from _price_new_trades_seq: {:?}", e);
		        },
	        }
        }
    }

    /// price trades that are coming on the trade receiver on
    /// spark, by doing repeated loops
    #[instrument]
    async fn _price_new_trades_spark(
        &self,
        trade_receiver: &mut Receiver<AOTradeRep>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> (PortfolioType, TradeRep<AOTradeRep>) {
        let new_trades = self._get_trades_from_recv(trade_receiver);

        let pricing_client = reqwest::Client::new();

        let pricing_result =
            self.price_trades_spark_2(&new_trades, &pricing_client, curr_new_mkt, metric);

        match pricing_result.await {
            Ok(pr) => (pr, new_trades),
            Err(_) => (PortfolioType::default(), new_trades),
        }
    }

    /// gets trades from receiver and constructs a trade
    /// representation from them.
    fn _get_trades_from_recv<TR: Clone + BaseTrade>(
        &self,
        trade_receiver: &mut Receiver<TR>,
    ) -> TradeRep<TR> {
        let mut new_trades = TradeRep::<TR>::default();

	    loop {
            match trade_receiver.try_recv() {
		        Ok(trade) => {
		            new_trades += &trade;
		        },
		        Err(e) => {
		            match e {
			            TryRecvError::Empty => {
			                return new_trades;
			            },
			            TryRecvError::Disconnected => {
			                return new_trades;  // TODO: CEHCK THIS
			            },
		            }
		        },
	        }
        }
    }
}
