// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'


use time::{Date, Month};

mod trade;
mod encdec;

mod controller;
use controller::Controller;

fn main() {
    env_logger::init(); // Start w/ RUST_LOG=debug cargo r

    let market_date = Date::from_calendar_date(2016, Month::January, 1).unwrap();

    let controller2 = Controller::new_from_config(
        market_date,
        "/home/brumen/work/rm/configuration.yaml".to_owned(),
    )
    .unwrap();

    // TODO: The topics should be read from config as well.
    controller2.start(
        "air_options.ao.option_positions".to_owned(),
        "mkt_events".to_owned(),
        "air_options.ao.results".to_owned(),
    );
}
