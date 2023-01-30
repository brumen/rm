// Starts the controller.

use std::collections::HashMap;
use std::time::Duration;
use time::{Date, Month};

mod controller;
use controller::Controller;

fn main() {
    env_logger::init(); // Start w/ RUST_LOG=debug cargo r

    let controller = Controller::new(
        Date::from_calendar_date(2016, Month::January, 1).unwrap(),
        "localhost".to_string(),
        Some(HashMap::<String, f64>::new()),
        "localhost:5010".to_string(),
        "localhost".to_string(), // market topic
        9092,
    );

    controller.start();
}
