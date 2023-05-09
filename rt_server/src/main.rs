// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'

use std::env::{args, };

use time::{Date, Month};

mod trade;
mod encdec;
mod portfolio;
mod pricer;

mod controller;
use controller::Controller;

mod trader;

use trader::LETFTrader;

fn main() {

    // main_risk()
    main_trader()

}

fn main_risk() {
    env_logger::init(); // Start w/ RUST_LOG=debug cargo r

    let market_date = Date::from_calendar_date(2016, Month::January, 1).unwrap();
    let option_type = "ao".to_string();
    let config_file : String = args().nth(1).unwrap();

    let controller2 = Controller::new_from_config(
        market_date,
        option_type,
        config_file,
        //"/home/brumen/work/rm/configuration.yaml".to_owned(),
    ).unwrap();

    // TODO: The topics should be read from config as well.
    controller2.start(
        "air_options.ao.option_positions".to_owned(),
        "mkt_events".to_owned(),
        "air_options.ao.results".to_owned(),
    );
}


//
// start trader w/ RuST_LOG=debug cargo r "CONFIG FILE"
fn main_trader() {

    env_logger::init(); // Start w/ RUST_LOG=debug cargo r

    let market_date = Date::from_calendar_date(2016, Month::January, 1).unwrap();
    let config_file : String = args().nth(1).unwrap();

    let trader = LETFTrader::new_from_config(
        market_date,
        config_file,
        //"/home/brumen/work/rm/configuration.yaml".to_owned(),
    ).unwrap();

    // TODO: The topics should be read from config as well.
    trader.start(
        "letf.positions".to_owned(),
        "letf.mkt".to_owned(),
        "letf.results".to_owned(),
    );

}
