// Starts the controller for Leveraged ETF trading..
use std::sync::{Arc, Mutex};

use crate::engine::CalcController;
use crate::market;
use crate::market::{MarketType, LETFP};
use crate::pricer::MarketPricingOptions;
use crate::rm_local::{RTRMConfig, RTRMLocal};
use crate::trader::LETFHedger;

#[allow(dead_code)]
pub async fn main_letf_trader() {
    letf_risk().await;
}

// start trader w/ RuST_LOG=debug cargo r "CONFIG FILE"

/// starts the risk engine of the letf trader.
async fn letf_risk() {
    // at some point add: //args().nth(1).unwrap();
    let config_file: String =
        "/home/brumen/work/rm/configs/configuration_letf_risk.yaml".to_owned();
    let rtrm_local = RTRMLocal::new_from_config(config_file.clone()).unwrap();

    let config_trader_f = std::fs::File::open(config_file).unwrap();
    let config_map: RTRMConfig = serde_yaml::from_reader(config_trader_f).unwrap();

    let market_pricing_options = MarketPricingOptions {
        pricing_server: "localhost:8001".to_owned(), // config_map.pricing_server.to_owned(), // "localhost:8000"
        pricing_endpoint: "pv".to_owned(),           //config_map.metric.to_owned(),  // "pv"
    };

    tokio_scoped::scope(|scope| {
        scope.spawn(rtrm_local.hedge("letf.positions".to_string(), "letf.results".to_string()));
        scope.spawn(rtrm_local.start(
            config_map.results_topic,
            config_map.mkt_topic,
            config_map.risk_topic,
            market::MktMsgParams::LETFParams(LETFP {
                curr_mkt: Arc::new(Mutex::new(MarketType::new())),
            }),
            &market_pricing_options,
        ));
    });
}
