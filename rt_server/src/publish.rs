use log::{info, debug, warn,};
use kafka::producer::{Producer, Record, RequiredAcks,};
use std::sync::mpsc::Receiver;
use std::thread::sleep;
use std::time::Duration;


use crate::pricer::PricingMetric;
use crate::portfolio::PortfolioType;
use crate::streaming::Streaming;


pub trait PublishResults : Streaming {

    fn metric(&self) -> PricingMetric;

    fn _publish_results(
        &self,
        curr_portfolio_recv: Receiver<PortfolioType>,
        results_topic: String,
    ) {
        let bootstrap_servers = format!("{}:{}", self.kafka_server_name(), self.kafka_port());

	let mut res_publisher = connect_with_retries_producer(&bootstrap_servers);

        loop {
            debug!("_publish_results: Publishing loop.");
            let curr_portfolio_raw = curr_portfolio_recv.recv();
            let curr_portfolio = match curr_portfolio_raw {
                Ok(curr_portfolio_actual) => {
                    debug!("_publish_results: Found actual portfolio: {:?}", curr_portfolio_actual);
                    curr_portfolio_actual
                },
                Err(e) => {
                    debug!("_publish_results: Error in publishing: {:?}", e);
                    continue;
                },
            };
            let curr_mkt_json = serde_json::ser::to_string(&curr_portfolio).unwrap();
            let curr_mkt_pv = format!("{{\"{}\": {}}}", self.metric(), curr_mkt_json);

            // implements bytearray(str(dumps(self.curr_market)), ascii))
            let market_record = Record::from_value(&results_topic, curr_mkt_pv.as_bytes())
                .with_partition(0);

            info!("_publish_results: Publishing new portfolio w/ {} trades.", curr_portfolio.keys().len());
            let _ = res_publisher.send(&market_record);
        }
    }
}


/// connects the consumer to Kafka 
pub fn connect_with_retries_producer(bootstrap_servers: &String) -> Producer {
    let mut listener_connected = false;
    let mut eventual_listener = None;

    while !listener_connected {

        match Producer::from_hosts(vec![bootstrap_servers.clone(),])
            .with_required_acks(RequiredAcks::One)
            .create()
	{
	    Ok(pos_listener) => {
		eventual_listener = Some(pos_listener);
		listener_connected = true;
	    },
	    Err(e) => {
		warn!("__construct_portfolio: listener is not connected, waiting 5 secs: {:?}", e);
		sleep(Duration::new(5, 0));
		eventual_listener = None;
	    },
	};
    }
    eventual_listener.unwrap()
}
