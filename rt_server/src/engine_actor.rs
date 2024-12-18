
use ractor::Actor;
use tokio::task::{JoinError, JoinHandle};

use crate::market::MktMsgParams;
use crate::mkt_handler_actor::MarketProducer;
use crate::portfolio_sender::connect_with_retries_rd;
use crate::pricer::{MarketPricingOptions, PricingMetric};


use crate::publish::connect_with_retries_producer_rd;
use crate::trade_sender::TradeProducer;
use crate::processor_curr::ProcessorCurr;
use crate::processor_new::ProcessorNew;
use crate::processor_bulk::ProcessorBulk;


pub async fn start2(
    metric: PricingMetric,
    pos_topic: String,     // position topic on kafka
    mkt_topic: String,     // market topic
    results_topic: String, // publish the results topic
    mkt_params: MktMsgParams,
    pricing_options: &MarketPricingOptions,
) -> Vec<JoinHandle<()>> {

    let (_processor_bulk_a, processor_bulk_handle) = Actor::spawn(
	None,
	ProcessorBulk {
	    metric,
	    pricing_options: (*pricing_options).clone()
	},
	(),
    ).await
    .expect("Could not start bulk processor");

    let kafka_bootstrap = "localhost:9010".to_string();
    //let kafka_bootstrap = format!("{}/{}", 1)
    let result_publisher = connect_with_retries_producer_rd(
	&kafka_bootstrap
    );
    
    let (_processor_curr_a, processor_curr_handle) = Actor::spawn(
	None,
	ProcessorCurr {
	    metric,
	    pricing_options: (*pricing_options).clone(),
	    result_publisher,
	},
	(),
    ).await
    .expect("Could not start current processor");
    
    let (_processor_new_a, processor_new_handle) = Actor::spawn(
	None,
	ProcessorNew {
	    metric,
	    pricing_options: (*pricing_options).clone(),
	    processor_curr: _processor_curr_a.clone(),
	    processor_bulk: _processor_bulk_a,
	},
	(),
    ).await
    .expect("Could not start new processor");

    let (_mkt_producer_a, mkt_producer_handle) = Actor::spawn(
	None,
	MarketProducer {
	    metric,
	    pricing_options: (*pricing_options).clone(),
	    mkt_listener: connect_with_retries_rd(
		&kafka_bootstrap, &mkt_topic
	    ),
	    new_processor: _processor_new_a.clone(),
	},
	(),
    ).await
    .expect("Could not start market producer");
    
    let kafka_server = "localhost".to_string();
    let kafka_port = 9010.to_string();
    let trade_producer = TradeProducer::new(
	kafka_server,
	kafka_port,
	pos_topic,
	_processor_curr_a,
	_processor_new_a,
    );
    
    let (_trade_capture_a, trade_capture_handle) = Actor::spawn(
	None, trade_producer, ()
    ).await
    .expect("Could not start trade producer");

    vec![
	trade_capture_handle,
	processor_curr_handle,
	processor_new_handle,
	processor_bulk_handle,
	mkt_producer_handle,
    ]
    
}
