// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'

//use std::env::args;

use std::thread;
use std::sync::{Arc, Mutex,};

use crate::market::{LETFP, MarketType, };
use crate::trader::{LETFTrader, RTConfig, };
use crate::rm_local::{RTRMLocal, RTRMConfig, };
use crate::engine::CalcController;
use crate::market;
use crate::pricer::MarketPricingOptions;

pub fn main_letf_trader() {

    thread::scope(|s| {
        let _ = thread::Builder::new()
            .name("letf_trader".to_string())
            .spawn_scoped(s, move || {
                letf_trader();
            })
            .unwrap();

        let _ = thread::Builder::new()
            .name("letf_risk".to_string())
            .spawn_scoped(s, move || {
                letf_risk();
            })
            .unwrap();

    });

}


// start trader w/ RuST_LOG=debug cargo r "CONFIG FILE"

/// starts the trader portion of the Leveraged ETF.
fn letf_trader() {

    let config_file : String = "/home/brumen/work/rm/configs/configuration_letf_trader.yaml".to_owned();

    let trader = LETFTrader::new_from_config(
        config_file.clone(),
    ).unwrap();

    let config_trader_f = std::fs::File::open(config_file).unwrap();
    let config_map: RTConfig = serde_yaml::from_reader(config_trader_f).unwrap();

    trader.start (
        config_map.positions_topic,  //"letf.positions".to_owned(),
        config_map.mkt_topic,  //"letf.mkt".to_owned(),
        config_map.results_topic,  // "letf.results".to_owned(),
    );

}


/// starts the risk engine of the letf trader.
fn letf_risk() {

    // at some point add: //args().nth(1).unwrap();
    let config_file : String = "/home/brumen/work/rm/configs/configuration_letf_risk.yaml".to_owned();
    let rtrm_local = RTRMLocal::new_from_config(
        config_file.clone(),
    ).unwrap();

    let config_trader_f = std::fs::File::open(config_file).unwrap();
    let config_map: RTRMConfig = serde_yaml::from_reader(config_trader_f).unwrap();

    let market_pricing_options = MarketPricingOptions {
        pricing_server: "localhost:8001".to_owned(),  // config_map.pricing_server.to_owned(), // "localhost:8000"
        pricing_endpoint: "pv".to_owned(),  //config_map.metric.to_owned(),  // "pv"
    };


    rtrm_local.start (
        config_map.positions_topic,
        config_map.mkt_topic,  // "letf.mkt".to_owned(),
        //config_map.risk_topic, // "letf.risk".to_owned(),
        config_map.results_topic,  // "letf.results".to_owned(),
        market::MktMsgParams::LETFParams(
            LETFP {
                curr_mkt: Arc::new(Mutex::new(MarketType::new()))
            }
        ),
        &market_pricing_options,

    );
}
