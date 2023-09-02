
// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'

//use std::env::args;

use crate::trade::AOTrade;
use crate::controller::Controller;
use crate::engine::CalcController;
use crate::market;


pub fn ao_main_risk() {

    // let market_date = Date::from_calendar_date(2016, Month::January, 1).unwrap();
    // let option_type = "ao".to_string();
    // let config_file : String = args().nth(1).unwrap();
    let config_file = "/home/brumen/work/rm/configs/configuration.yaml".to_owned();

    let controller2 = Controller::new_from_config(
        config_file,
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
