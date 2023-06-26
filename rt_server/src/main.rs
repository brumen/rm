// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'

use std::env::args;
use std::thread;
use std::sync::{Arc, Mutex,};

mod trade;
use trade::AOTrade;

mod encdec;
mod portfolio;
mod pricer;
mod market;
use market::{LETFP, MarketType,};
mod ref_deref;

mod controller;
use controller::Controller;

mod trader;
use trader::LETFTrader;

mod publish;
mod streaming;
mod trade_processor;

mod rm_local;
use rm_local::RTRMLocal;

mod engine;
mod mkt_handler;

mod portfolio_sender;

use crate::engine::CalcController;


fn main() {

    // main_risk()

    env_logger::init();

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

#[allow(dead_code)]
fn main_risk() {
    env_logger::init(); // Start w/ RUST_LOG=debug cargo r

    // let market_date = Date::from_calendar_date(2016, Month::January, 1).unwrap();
    //let option_type = "ao".to_string();
    let config_file : String = args().nth(1).unwrap();

    let controller2 = Controller::new_from_config(
        config_file,
        //"/home/brumen/work/rm/configuration.yaml".to_owned(),
    ).unwrap();

    // TODO: The topics should be read from config as well.
    <Controller as CalcController<AOTrade>>::start (
	&controller2,
        "air_options.ao.option_positions".to_owned(),
        "mkt_events".to_owned(),
        "air_options.ao.results".to_owned(),
        market::MktMsgParams::AOParams(),
    );
}


// start trader w/ RuST_LOG=debug cargo r "CONFIG FILE"

/// starts the trader portion of the Leveraged ETF.
fn letf_trader() {

    let config_file : String = "/home/brumen/work/rm/configs/configuration_letf_trader.yaml".to_owned();   //args().nth(1).unwrap();

    let trader = LETFTrader::new_from_config(
        config_file,
    ).unwrap();

    // TODO: The topics should be read from config as well.
    trader.start (
        "letf.positions".to_owned(),
        "letf.mkt".to_owned(),
        "letf.results".to_owned(),
    );

}


/// starts the risk engine of the letf trader.
fn letf_risk() {

    // at some point add: //args().nth(1).unwrap();
    let config_file : String = "/home/brumen/work/rm/configs/configuration_letf_risk.yaml".to_owned();
    let rtrm_local = RTRMLocal::new_from_config(
        config_file,
    ).unwrap();

    // TODO: The topics should be read from config as well.
    rtrm_local.start (
        "letf.results".to_owned(),
        "letf.mkt".to_owned(),
        "letf.risk".to_owned(),
        market::MktMsgParams::LETFParams(
            LETFP {
                curr_mkt: Arc::new(Mutex::new(MarketType::new()))
            }
        ),
    );
}
