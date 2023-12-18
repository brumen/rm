use tracing::{
    debug,
    info,
    warn,
    info_span,
    debug_span,
    Instrument,
};
use serde::{Deserialize, Serialize};
use kafka::consumer::Message;
//use reqwest::{self, Error};
use std::collections::HashMap;
use std::sync::mpsc::{Sender, Receiver, };
use core::convert::From;
use std::sync::{Arc, Mutex};
use tokio::runtime;


use crate::ao_trade::{
    AOTrade,
    AOTradeRep,
};
use crate::trade::{
    BaseTrade,
    TradeDirection,
    TradeRep,
    TradeReduce,
};
use crate::portfolio::{
    PortfolioType,
    PricingResults,
};
use crate::market::{
    MarketType,
    CurrNewMarket,
    MktMsgParams,
    MarketSwitching,
    TradeMarketDiscovery,
};
use crate::ref_deref::TryFromRef;
use crate::pricer::{
    PricingMetric,
    PricingStruct,
    MarketPricingOptions,
    PriceTradeAsync,
    Decoder,
    RestPricerSpark,
};
use crate::publish::PublishResults;
use crate::streaming::Streaming;
use crate::mkt_handler::MktEventHandler;
use crate::trade_procs::{ProcessTradeAsync, RiskProcessors,};


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
    async_rt: runtime::Runtime,
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
        self.kafka_server_name.clone()  // TODO: CHECK IF THIS CAN BE REMOVED HERE!!!
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

    fn _handle_mkt_msg(
        &self,
        mkt_msg : &Message,
        new_mkt_sender: Sender<MarketType>,
        _mkt_msg_params: MktMsgParams,
    ) {

        //let _handle_msg_span = info_span!(
        //    "Handling mkt message",
        //);

        let optional_mkt = MarketType::try_from_ref(mkt_msg);

	    let market_obj = match optional_mkt {
	        Err(e) => {
		        warn!("_handle_mkt_msg: Error converting to market object from json: {:?}", e);
		        return;
	        },
	        Ok(market_inside) => {
                debug!("_handle_mkt_msg: Market = {:?}", market_inside);
                market_inside
            },
	    };

        let mkt_client_address = format!(
            "http://{}/future_market",
            self.trade_pricer.clone(),
        );
        let market_posted = reqwest::blocking::Client::new()
            .post(format!("{0}", mkt_client_address))
            .json(&HashMap::from([("market", &market_obj)]))
            .send();

        match market_posted {
            Ok(_) => {
                debug!("_handle_mkt_msg: Market posted successfully.");
            },
            _ => {
                warn!("_handle_mkt_msg: Could not post the market successfully. Ignoring last market.");
            }
        }

        let _ = new_mkt_sender.send(market_obj);
    }
}


impl MarketSwitching for Controller {
    /// switch markets on the trade api.
    fn _switch_markets(&self) {
        info!("_switch_markets: Switching markets: current <- new.");
        let _ = reqwest::blocking::get(format!(
            "http://{}/switch_markets",
            self.trade_pricer
        ));
    }

    fn _curr_mkt(&self) -> Arc<Mutex<MarketType>> {
	    self.curr_mkt.clone()
    }

    fn _new_mkt(&self) -> Arc<Mutex<MarketType>> {
	    self.new_mkt.clone()
    }
}


impl TradeMarketDiscovery for Controller
//where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade
{ }


// TradeReduce reduces the trade to empty,
// we dont need any additional information from the trade.
impl TradeReduce for Controller {
    type ReductionType = AOTradeRep;
    type TradeType = AOTrade;

    fn reduce(&self, trade: &Self::TradeType) -> Self::ReductionType {
        AOTradeRep(trade.id())
    }
}


impl<TR> ProcessTradeAsync<TR> for Controller
//where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send + Sync
where TR: PriceTradeAsync + BaseTrade
{
    async fn _process_trade(
        &self,
        trade: &TR,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType {

        let trade_id = trade.id();
        // let trade_direction = tr.direction();

        //let _process_trade_span = debug_span!(
        //     "_process trade span",
        //    %trade_id,
        //);

        // let _ = _process_trade_span.enter();

        let trade_v = trade.value_by_metric(
            metric,
            pricing_options,
            curr_new_mkt,
        ) //.instrument(_process_trade_span)
            .await;

        let trade_portf = match trade_v {
            PricingResults::PV(pv) => pv,
            PricingResults::PV01(pv01) => pv01.aggregate(),
            PricingResults::PnL(pnl) => pnl,
        };

        trade_portf

        // let mut cp = curr_portfolio.lock().unwrap();
        //TradeDirection::Create => *cp += trade_portf,

        //match trade_direction {
        //    TradeDirection::Create => trade_portf,
        //    TradeDirection::Delete => - trade_portf,
        //    _ => todo!(),
        //}
    }
}

impl RiskProcessors for Controller
//where
//    TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send + Sync,
{
    #[tracing::instrument]
    fn _price_existing_trades(
        &self,
        all_trades: &TradeRep<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType {

        //if all_trades.len() > 20 {
            // send to spark.
            return self.price_trades_on_spark(
                all_trades,
                metric,
                pricing_options,
                curr_new_mkt,
            )
        //}

        // price them sequentially
        //self._price_new_trades_seq(
        //    all_trades,
        //    metric,
        //    pricing_options,
        //     curr_new_mkt,
        //   )
    }

    #[tracing::instrument]
    fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &Receiver<Self::TR>,
        all_trades: &mut TradeRep<Self::TR>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<Self::TR>)>,
    ) {

        self._price_new_trades_seq (
            curr_portfolio,
            trade_receiver,
            all_trades,
            metric,
            pricing_options,
            curr_new_mkt,
            new_trades_sender,
        )

    }
}

// implementation of decoder for results on the portfolio.
impl Decoder for Controller {}

impl RestPricerSpark<AOTradeRep> for Controller {
    fn _pricing_server_spark(&self) -> String {
        self.trade_pricer.to_owned()  // TODO: THIS IS GARBAGE
    }

    fn _pricing_endpoint_spark(
        &self,
        market_ : CurrNewMarket,
        metric: PricingMetric,
    ) -> String {
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

        let (num_tokio_worker_threads, max_tokio_blocking_threads) = (8, 512); // 512 is tokio's current default
        let rt = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(8 * 1024 * 1024)
            .worker_threads(num_tokio_worker_threads)
            .max_blocking_threads(max_tokio_blocking_threads)
            .build()
            .unwrap();

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
            async_rt: rt,
        }
    }

    /// constructs the controller from configuration read from the file.
    /// config_file. If it cant read the file properly, it crashes.
    pub fn new_from_config(
        config_file: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
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

    /// prices trades sequentially.
    #[tracing::instrument]
    fn _price_new_trades_seq (
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &Receiver<AOTradeRep>,
        all_trades: &mut TradeRep<AOTradeRep>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<AOTradeRep>)>,
    )
    // where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + Send + Sync
    {

        self.async_rt.block_on (
            async {
                let mut trade_counter = 0;
                while let Ok(trade) = trade_receiver.try_recv() {
                    trade_counter += 1;
                    if trade_counter > 20 {
                        break;
                    }
                    *all_trades += &trade;

                    *curr_portfolio += self._process_trade(
                        &trade,
                        metric,
                        pricing_options,
                        curr_new_mkt,
                    ).await;

                    let _ = new_trades_sender.send(
                        (curr_portfolio.clone(), TradeRep(all_trades.clone()))
                    );
                }
            }
        );
    }


    /// price trades that are coming on the trade receiver on
    /// spark, by doing repeated loops
    #[tracing::instrument]
    fn _price_new_trades_spark(
        &self,
        trade_receiver: &Receiver<AOTradeRep>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> (PortfolioType, TradeRep<AOTradeRep>) {

        let new_trades = self._get_trades_from_recv(trade_receiver);

        let pricing_client = reqwest::blocking::Client::new();

        let pricing_result = self.price_trades_spark_2(
	        &new_trades,
	        &pricing_client,
            curr_new_mkt,
            metric
        );

        match pricing_result {
            Ok(pr) => (pr, new_trades),
            Err(_) => (PortfolioType::new(), new_trades),
        }

    }

    // gets trades from receiver and constructs a trade
    // representation from them.
    fn _get_trades_from_recv<TR: Clone + BaseTrade> (
        &self,
        trade_receiver: &Receiver<TR>,
    ) -> TradeRep<TR> {

        let mut new_trades = TradeRep::<TR>::new();

        for trade in trade_receiver.try_iter() {
            new_trades += &trade;
        }

        new_trades
    }
}
