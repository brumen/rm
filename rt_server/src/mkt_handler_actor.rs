use rdkafka::consumer::{CommitMode, Consumer};
use tracing::warn;
use rdkafka::consumer::StreamConsumer;

use ractor::{cast, async_trait, Actor, ActorRef, ActorProcessingErr};

use crate::pricer::MarketPricingOptions;
use crate::trade::TradeRep;
use crate::pricer::PricingMetric;
use crate::market::{MarketType, MktMsgParams};
use crate::portfolio_sender::connect_with_retries_rd;
use crate::portfolio::PortfolioType;
use crate::ref_deref::TryFromRef;


pub struct MarketProducer<'a, ReductionType>{
    metric: PricingMetric,
    pricing_options: &'a MarketPricingOptions,
    all_trades: TradeRep<ReductionType>,
    curr_portfolio: PortfolioType,
    mkt_listener: StreamConsumer,  // listening for market events.
}


#[async_trait]
impl<'a, ReductionType> Actor for MarketProducer<'a, ReductionType>
where ReductionType: Send + Sync
{
    type Msg = MarketType;
    type State = ();
    type Arguments = (String, String, String);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {

	let (server_name, server_port, mkt_topic) = args;
	
        let bootstrap_servers = format!("{}:{}", server_name, server_port);	
        self.mkt_listener = connect_with_retries_rd(&bootstrap_servers, &mkt_topic);

	let new_mkt_msg = self.mkt_listener.recv().await?;
	let new_mkt = MarketType::try_from_ref(&new_mkt_msg)?;
	cast!(myself, new_mkt);
	
	Ok(())
    }

    async fn handle(
        &self,
	myself: ActorRef<Self::Msg>,
	message: Self::Msg,
	state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

        let market_obj = MarketType::try_from_ref(&message)?;

	// TODO: WHAT TO DO W/ THIS INFORMATION???
	// THIS IS WRONG!!!
        **(self
            ._future_mkt()
            .lock()
            .expect("Could not lock self.future_mkt")) = market_obj.clone(); // TODO: IS THIS CLONE NECESSARY???
        if let Err(e) = new_mkt_sender.send(market_obj).await {
            warn!("Could not send a message to the new market: {:?}", e);
        }

        // match mkt_listener_.commit_message(&borrowed_msg, CommitMode::Sync) {
        //     Ok(_) => {
        //         debug!("Successful commit of market message");
        //     }
        //     Err(e) => {
        //         error!("Message could not be committed to Kafka. Continuing in best hopes: {:?}", e);
        //     }
        // }

	Ok(())
    }

}
