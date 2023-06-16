use log::{info, debug, };
use kafka::producer::{Producer, Record, RequiredAcks,};
use std::sync::mpsc::Receiver;

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

        let mut res_publisher = Producer::from_hosts(vec![bootstrap_servers,])
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        loop {
            info!("PUBLISHING RESULTS");
            let curr_portfolio_raw = curr_portfolio_recv.recv();
            let curr_portfolio = match curr_portfolio_raw {
                Ok(curr_mkt_actual) => {
                    debug!("FOUND ACTUAL MARKET");
                    curr_mkt_actual
                },
                Err(_) => {
                    debug!("COULD NOT PUBLISH ANYTHING");
                    continue;
                },
            };
            let curr_mkt_json = serde_json::ser::to_string(&curr_portfolio).unwrap();
            let curr_mkt_pv = format!("{{\"{}\": {}}}", self.metric(), curr_mkt_json);

            // implements bytearray(str(dumps(self.curr_market)), ascii))
            let market_record = Record::from_value(&results_topic, curr_mkt_pv.as_bytes())
                .with_partition(0);

            info!("PUBLISHING: Publishing new portfolio w/ {} trades.", curr_portfolio.keys().len());
            let _ = res_publisher.send(&market_record);
        }
    }
}
