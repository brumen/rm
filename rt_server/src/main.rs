// Starts the controller.

use std::collections::HashMap;
use std::time::Duration;
use time::{Date, Month};

mod trade;
mod encdec;

mod controller;
use controller::Controller;

fn main() {
    env_logger::init(); // Start w/ RUST_LOG=debug cargo r

    let market_date = Date::from_calendar_date(2016, Month::January, 1).unwrap();
    let kafka_server_name = "localhost".to_string();
    let kafka_port = 9092;
    let trade_pricer = "localhost:5010".to_string();

    let controller = Controller::new(
        market_date,
        Some(HashMap::from([
            ("nb_sim".to_owned(), 5000 as f64),
            ("default_price".to_owned(), 200.),
        ])),
        kafka_server_name.to_owned(),
        9092,
        trade_pricer.to_owned(),
    );

    let controller2 = Controller::new_from_config(
        market_date,
        kafka_server_name.to_owned(),
        kafka_port,
        trade_pricer.to_owned(),
        "/home/brumen/work/rm/configuration.yaml".to_owned(),
    )
    .unwrap();

    controller2.start(
        "air_options.ao.option_positions".to_owned(),
        "mkt_events".to_owned(),
        "air_options.ao.results".to_owned(),
    );
}
