// Starts the controller for Leveraged ETF trading..
use std::sync::{Arc, Mutex};

use crate::engine::CalcController;
use crate::market;
use crate::market::{MarketType, LETFP};
use crate::pricer::MarketPricingOptions;
use crate::rm_local::{RTRMConfig, RTRMLocal};
use crate::trader::{LETFHedger};
use crate::portfolio_sender::connect_with_retries_rd;
use tracing::info;
use rdkafka::consumer::{Consumer, CommitMode};
use futures::future::join_all;
use core::time::Duration;

#[allow(dead_code)]
pub async fn main_letf_trader() {

    // tokio_scoped::scope(
    //     |scope| {
    //         scope.spawn(
    //             letf_trader()
    //         );
    //          scope.spawn(
    letf_risk().await;
    //);
    //    }
    //);
}

// start trader w/ RuST_LOG=debug cargo r "CONFIG FILE"

/// starts the trader portion of the Leveraged ETF.
// async fn letf_trader() {
//     let config_file: String =
//         "/home/brumen/work/rm/configs/configuration_letf_trader.yaml".to_owned();

//     let trader = LETFTrader::new_from_config(config_file.clone()).unwrap();

//     let config_trader_f = std::fs::File::open(config_file).unwrap();
//     let config_map: RTConfig = serde_yaml::from_reader(config_trader_f).unwrap();

//     trader.start(
//         config_map.positions_topic,
//         config_map.mkt_topic,
//         config_map.results_topic,
//     ).await;


// }


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

    tokio_scoped::scope(
        |scope| {
            scope.spawn(
                rtrm_local.hedge(
                    "letf.positions".to_string(),
                    "letf.results".to_string(),
                )
            );
            scope.spawn(
                rtrm_local.start(
                    config_map.results_topic,
                    config_map.mkt_topic,
                    config_map.risk_topic,
                    market::MktMsgParams::LETFParams(
                        LETFP {
                            curr_mkt: Arc::new(Mutex::new(MarketType::new())),
                        }
                    ),
                    &market_pricing_options,
                )
            );
        }
    );

    // let l1 = tokio::spawn(position_l());
    // let l2 = tokio::spawn(position_l2());

    // let (a,b) = tokio::join!(l1, l2);

    // info!("A {:?}", a);
    // info!("B {:?}", b);

    // join_all(vec![position_l(), position_l2()]).await;
    // tokio_scoped::scope(
    //     |scope| {
    //         scope.spawn(position_l());
    //         scope.spawn(position_l2());
    //     }
    // );

}

// fn position_l() -> impl std::future::Future<Output=()> + Send {
//     async move {
//         let bs = "localhost:9092".to_string();
//         let pos_topic = "letf.positions".to_string();
//         let position_listener = connect_with_retries_rd(&bs, &pos_topic);
//         let mut x = 1;

//         loop {
//             let trade = position_listener.recv().await.unwrap();
//             info!("A: {:?}", trade);
//             let _ = tokio::time::sleep(Duration::new(1, 0)).await;
//             info!("A, {:?}", x);

//             x += 1;

//             //let _ = position_listener.commit_message(&trade, CommitMode::Async);
// 		}
//     }
// }


// fn position_l2() -> impl std::future::Future<Output=()> + Send {
//     async move {
//         let bs = "localhost:9092".to_string();
//         let pos_topic = "letf.positions".to_string();
//         let position_listener = connect_with_retries_rd(&bs, &pos_topic);

//         let mut x = 1;
//             loop {
//                 let trade = position_listener.recv().await.unwrap();
//                 info!("B: {:?}", trade);

//                 let _ = tokio::time::sleep(Duration::new(1, 0)).await;
//                 info!("B, {:?}", x);

//                 x += 1;

//                 //let _ = position_listener.commit_message(&trade, CommitMode::Async);
// 		    }
//         }
//     }
