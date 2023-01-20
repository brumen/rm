// Starts the server.

use std::collections::HashMap;
use std::time::Duration;
use time::{Date, Month};

mod controller_async;
use controller_async::Controller;

fn main() {
    let mut controller = Controller::new(
        Date::from_calendar_date(2016, Month::January, 1).unwrap(),
        "localhost".to_string(),
        9092,
        "mkt_events".to_string(), // market topic
        "air_options.ao.option_positions".to_string(),
        "results".to_string(),
        Some(HashMap::<String, f64>::new()),
        "localhost:5010".to_string(),
    );

    controller.start(Duration::from_secs(1), Duration::from_secs(1));
}
