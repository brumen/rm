// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'

//use std::env::args;

use std::thread;
use std::sync::{Arc, Mutex,};

use crate::market::{LETFP, MarketType,};
use crate::trader::LETFTrader;
use crate::rm_local::RTRMLocal;
use crate::engine::CalcController;
use crate::market;


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
