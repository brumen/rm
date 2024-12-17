use futures::future::join_all;

use ractor::Actor;
use tokio::task::JoinError;

use crate::market::MktMsgParams;
use crate::pricer::{MarketPricingOptions, PricingMetric};


use crate::trade_sender::TradeProducer;
use crate::processor_curr::ProcessorCurr;
use crate::processor_new::ProcessorNew;
use crate::processor_bulk::ProcessorBulk;


async fn start2(
    metric: PricingMetric,
    pos_topic: String,     // position topic on kafka
    mkt_topic: String,     // market topic
    results_topic: String, // publish the results topic
    mkt_params: MktMsgParams,
    pricing_options: &MarketPricingOptions,
) -> Vec<Result<(), JoinError>> {

    let (_processor_bulk_a, processor_bulk_handle) = Actor::spawn(
	None,
	ProcessorBulk { metric, pricing_options },
	(),
    ).await
    .expect("Could not start bulk processor");

    let (_processor_curr_a, processor_curr_handle) = Actor::spawn(
	None,
	ProcessorCurr {
	    metric,
	    pricing_options: pricing_options.clone(),
	    result_publisher: result_publisher,
	},
	(),
    ).await
    .expect("Could not start current processor");
    
    let (_processor_new_a, processor_new_handle) = Actor::spawn(
	None,
	ProcessorNew {
	    metric,
	    pricing_options,
	    processor_curr: _processor_curr_a,
	    processor_bulk: _processor_bulk_a,
	},
	(),
    ).await
    .expect("Could not start new processor");
    
    let trade_producer = TradeProducer::new(
	metric,
	kafka_server,
	kafka_port,
	pos_topic,
	*pricing_options,
	_processor_curr_a,
	_processor_new_a,
    );
    
    let (_trade_capture_a, trade_capture_handle) = Actor::spawn(
	None, trade_producer, ()
    ).await
    .expect("Could not start trade producer");

    let entire_engine = vec![
	trade_capture_handle,
	processor_curr_handle,
	processor_new_handle,
	processor_bulk_handle,
    ];
    
    // start all the actors, not sequentially
    let result = join_all(entire_engine).await;

    result
}


#[tokio::main]
async fn main() {

    let result = start2(TODO); // TODO: FINISH HERE!!!
    result.await.expect("Actor failed to exit cleanly");
}
