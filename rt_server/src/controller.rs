use tracing::{
    debug,
    info,
    warn,
    info_span,
    debug_span,
    Instrument,
};
use serde::{Deserialize, Serialize};
use futures::future;
use tokio;

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
//    mkt_client: reqwest::blocking::RequestBuilder,
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

        let _handle_msg_span = info_span!(
            "Handling mkt message",
        );

        debug!("_handle_mkt_msg: Entering routine!");
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
        let market_posted = reqwest::blocking::Client::new()  // TODO: THIS ALWAYS REPEATS!!!
        //let market_posted = client
        //let market_posted = self.mkt_client
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

impl<TT> ProcessTradeAsync<TT> for Controller
where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send
{
    #[tracing::instrument]
    async fn _process_trade(
        &self,
        trade: TT,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {

        let trade_id = trade.id();
        let trade_direction = trade.direction();

        let _process_trade_span = info_span!(
            "_process trade span",
            %trade_id,
        );

        let _ = _process_trade_span.enter();

        // TODO: curr_new_mkt dependecy missing here!!!
        let trade_v = trade.value_by_metric(
            metric,
            pricing_options,
            curr_new_mkt,
        ).instrument(_process_trade_span)
            .await;

        debug!("_process_trade: Trade value = {:?}", trade_v);
        let trade_portf = match trade_v {
            PricingResults::PV(pv) => pv,
            PricingResults::PV01(pv01) => pv01.aggregate(),
            PricingResults::PnL(pnl) => pnl,
        };

        match trade_direction {
            TradeDirection::Create => *(curr_portfolio.lock().unwrap()) += trade_portf,
            TradeDirection::Delete => *(curr_portfolio.lock().unwrap()) -= trade_portf,
            _ => {},
        }

        let _ = curr_portfolio_sender.send(curr_portfolio.lock().unwrap().clone());
    }
}

impl<TT> RiskProcessors<TT> for Controller
where TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send + Sync
{

    #[tracing::instrument]
    fn _existing_trades(
        &self,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {

        // price all existsing trades in all_trades
        let mut trade_handles = vec![];
        for trade in all_trades.lock().unwrap().values() {
            trade_handles.push(
                self._process_trade(
                    trade.clone(),
                    metric,
                    pricing_options,
                    curr_portfolio.clone(),
                    curr_portfolio_sender,
                    curr_new_mkt,
                )
            );
        }

        // we have tasks in trade handles, run them all
        let _finished_futs = self.async_rt.block_on(async {
            let result = future::join_all(trade_handles);
            result.await
        });
    }

    #[tracing::instrument]
    fn _new_trades(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {

        let mut trade_handles: Vec<_> = vec![];
        while let Ok(trade) = trade_receiver.try_recv() {
            debug!("_trade_processor_curr: Received good trade {:?}", trade);

            let mut all_trades_local = all_trades.lock().unwrap();
            let new_trade = !all_trades_local.contains(&trade);
            if new_trade {
                all_trades_local.add_trade(trade.clone());
            }
            drop(all_trades_local);

            if new_trade {
                trade_handles.push(
                    self._process_trade(
                        trade.clone(),
                        metric,
                        pricing_options,
                        curr_portfolio.clone(),
                        curr_portfolio_sender,
                        curr_new_mkt,
                    )
                );
            }
        }

        // we have tasks in trade handles, run them all
        let _finished_futs = self.async_rt.block_on(async {
            let result = future::join_all(trade_handles);
            result.await
        });
    }

    #[tracing::instrument]
    fn _run_computations(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    ) {

        debug!("_trade_processor_new: Running existing trades.");
        // price all existsing trades in all_trades
        let mut trade_handles = vec![];
        {
            for trade in all_trades.lock().unwrap().values() {
                trade_handles.push(
                    self._process_trade(
                        trade.clone(),
                        metric,
                        pricing_options,
                        curr_portfolio.clone(),
                        curr_portfolio_sender,
                        curr_new_mkt,
                    )
                );
            }
        }

        // add new trades to the pipeline.
        while let Ok(trade) = trade_receiver.try_recv() {
            debug!("_trade_processor_curr: Received good trade {:?}", trade);

            let mut all_trades_local = all_trades.lock().unwrap();
            let new_trade = !all_trades_local.contains(&trade);
            if new_trade {
                all_trades_local.add_trade(trade.clone());
            }
            drop(all_trades_local);

            if new_trade {
                trade_handles.push(
                    self._process_trade(
                        trade.clone(),
                        metric,
                        pricing_options,
                        curr_portfolio.clone(),
                        curr_portfolio_sender,
                        curr_new_mkt,
                    )
                );
            }
        }

        // we have tasks in trade handles, run them all
        let _finished_futs = self.async_rt.block_on(async {
            let result = future::join_all(trade_handles);
            result.await
        });

    }
}
