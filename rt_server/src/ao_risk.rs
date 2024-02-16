// Starts the controller.

use crate::controller::{Controller, RTConfig};
use crate::engine::CalcController;
use crate::market;
use crate::pricer::MarketPricingOptions;

pub async fn ao_main_risk() {
    let config_file = "/home/brumen/work/rm/configs/configuration.yaml".to_owned();
    let config_f = std::fs::File::open(config_file.clone()).unwrap();
    let config_map: RTConfig = serde_yaml::from_reader(config_f).unwrap();

    let controller = Controller::new_from_config(config_file).unwrap();

    let position_topic = config_map.pos_topic.to_owned(); // "air_options.ao.option_positions"
    let mkt_topic = config_map.mkt_topic.to_owned(); // "air_options.ao.mkt_events"
    let results_topic = config_map.results_topic.to_owned(); // "air_options.ao.results"
    let market_pricing_options = MarketPricingOptions {
        pricing_server: config_map.pricing_server.to_owned(), // "localhost:8000"
        pricing_endpoint: config_map.metric.to_owned(),       // "pv"
    };

    controller.start(
        position_topic,
        mkt_topic,
        results_topic,
        market::MktMsgParams::AOParams(),
        &market_pricing_options,
    ).await;
}
