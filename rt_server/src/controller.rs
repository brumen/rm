use futures::future::join_all;
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
use reqwest;
use std::collections::HashMap;
use std::sync::mpsc::{Sender, Receiver, };
use core::convert::From;
use std::sync::{Arc, Mutex};
use tokio::runtime;

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
};
use crate::ref_deref::TryFromRef;
use crate::pricer::{
    PricingMetric,
    PricingStruct,
    MarketPricingOptions,
    PriceTradeAsync,
    Decoder,
    RestPricerSpark, self,
};
use crate::publish::PublishResults;
use crate::streaming::Streaming;
use crate::trade_processor::{MarketSwitching, TradeMarketDiscovery};
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

        let mkt_client_address = "http://localhost:8000/future_market";
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
        info!("Switching markets: current <- new.");
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


impl<TT> TradeMarketDiscovery<TT> for Controller
where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade
{ }


// TradeReduce reduces the trade to empty,
// we dont need any additional information from the trade.
impl<TT:BaseTrade> TradeReduce<TT> for Controller {
    type ReductionType = ();

    fn reduce(&self, trade: &TT) {
    }
}

impl<TT> ProcessTradeAsync<TT> for Controller
where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send + Sync
{

    #[tracing::instrument]
    async fn _process_trade(
        &self,
        trade: TT,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType {

        let trade_id = trade.id();
        let trade_direction = trade.direction();

        let _process_trade_span = debug_span!(
             "_process trade span",
            %trade_id,
        );

        let _ = _process_trade_span.enter();

        let trade_v = trade.value_by_metric(
            metric,
            pricing_options,
            curr_new_mkt,
        ).instrument(_process_trade_span)
            .await;

        let trade_portf = match trade_v {
            PricingResults::PV(pv) => pv,
            PricingResults::PV01(pv01) => pv01.aggregate(),
            PricingResults::PnL(pnl) => pnl,
        };

        // let mut cp = curr_portfolio.lock().unwrap();
        //TradeDirection::Create => *cp += trade_portf,

        match trade_direction {
            TradeDirection::Create => trade_portf,
            TradeDirection::Delete => - trade_portf,
            _ => todo!(),
        }
    }
}

impl<TT> RiskProcessors<TT> for Controller
where
    TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send + Sync,
{

    #[tracing::instrument]
    fn _price_existing_trades(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<Self::ReductionType>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType {

        self._price_new_trades_spark(
            trade_receiver,
            all_trades,
            metric,
            pricing_options,
            curr_new_mkt,
        )
    }

    #[tracing::instrument]
    fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<Self::ReductionType>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<PortfolioType>,
    ) {

        self._price_new_trades_on_service(
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

impl<TT> RestPricerSpark<TT> for Controller
where TT: PartialEq
{
    fn _pricing_server_spark(&self) -> String {
        "localhost:8000/".to_owned()
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
 //           mkt_client: rb,
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

    #[tracing::instrument]
    fn _price_existing_trades_on_spark<TR> (
        &self,
        all_trades: Arc<Mutex<TradeRep<TR>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        curr_portfolio_sender: &Sender<PortfolioType>,
    ) -> PortfolioType
    where TR: PartialEq + std::fmt::Debug + Clone + pricer::PriceTradeAsync + Send + Sync
    {

        let mut curr_portfolio = PortfolioType::new();

        // we have tasks in trade handles, run them all
        let _finished_futs = self.async_rt.block_on(
            async {
                let all_trades_l = all_trades.lock().unwrap();

                for trade in all_trades_l.values() {
                    curr_portfolio  += self._process_trade(
                        trade.clone(),
                        metric,
                        pricing_options,
                        curr_new_mkt,
                    ).await;
                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());
                }
            }
        );

        curr_portfolio
    }

    #[tracing::instrument]
    fn _price_new_trades_on_service_working<TT> (
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<()>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<PortfolioType>,
    )
    where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + Send + Sync
    {

        self.async_rt.block_on (
            async {
                let mut trade_counter = 0;
                while let Ok(trade) = trade_receiver.try_recv() {
                    trade_counter += 1;
                    if trade_counter > 20 {
                        break;
                    }
                    let mut all_trades_local = all_trades.lock().unwrap();
                    let new_trade = !all_trades_local.contains(&trade.id());
                    if new_trade {
                        self.add_trade(&trade, &mut (*all_trades_local));
                    }

                    if new_trade {
                        *curr_portfolio += self._process_trade(
                            trade,
                            metric,
                            pricing_options,
                            curr_new_mkt,
                        ).await;

                        let _ = new_trades_sender.send(curr_portfolio.clone());
                    }
                }
            }
        );
    }

    #[tracing::instrument]
    fn _price_new_trades_on_service<TT> (
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<()>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<PortfolioType>,
    )
    where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + Send + Sync
    {

        self.async_rt.block_on (
            async {
                let mut trade_counter = 0;
                let mut trade_future_pricers = vec![];

                while let Ok(trade) = trade_receiver.try_recv() {
                    trade_counter += 1;
                    if trade_counter > 20 {
                        break;
                    }

                    let mut all_trades_local = all_trades.lock().unwrap();
                    let is_new_trade = !all_trades_local.contains(&trade.id());
                    if is_new_trade {
                        self.add_trade(&trade, &mut (*all_trades_local));
                    }

                    trade_future_pricers.push(
                        self._process_trade(
                            trade,
                            metric,
                            pricing_options,
                            curr_new_mkt,
                        )
                    );
                }

                let results = join_all(trade_future_pricers).await;
                for trade_result in results {
                    *curr_portfolio += trade_result;
                }

                let _ = new_trades_sender.send(curr_portfolio.clone());
            }
        );
    }

    #[tracing::instrument]
    fn _price_new_trades_spark<TT>(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<()>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType
    where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + Send + Sync
    {

        let mut all_trades_local = all_trades.lock().unwrap();
        let new_trades = self._get_trades_from_recv(trade_receiver);
        *all_trades_local += &new_trades;

        let pricing_client = reqwest::blocking::Client::new();

        info!("Staring pricing on spark for {} trades", all_trades_local.len());
        self.price_trades_spark(
	        &all_trades_local,
	        &pricing_client,
            curr_new_mkt,
            metric
        )
    }

    fn _get_trades_from_recv<TT> (
        &self,
        trade_receiver: &Receiver<TT>,
    ) -> TradeRep<()>
    where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + Send + Sync
    {

        let mut new_trades = TradeRep::<()>::new();
        while let Ok(trade) = trade_receiver.try_recv() {
            self.add_trade(&trade, &mut new_trades);
        }

        new_trades
    }
}
