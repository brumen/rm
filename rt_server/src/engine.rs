use tokio::sync::mpsc::channel;
use tracing::{info, debug};
use rdkafka::message::BorrowedMessage;

use crate::market::MarketType;
use crate::market::MktMsgParams;
use crate::mkt_handler::MktEventHandler;
use crate::portfolio::PortfolioType;
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::{MarketPricingOptions, Decoder};
use crate::process_trade::ProcessTradeValue;
use crate::publish::PublishResults;
use crate::trade::{TradeRep, TradeReduce};
use crate::trade_procs::RiskProcessors;
use crate::ref_deref::TryFromRef;

pub trait CalcController : TradeReduce {
    fn start(
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
        mkt_params: MktMsgParams,
        pricing_options: &MarketPricingOptions,
    ) -> impl std::future::Future<Output=()> + Send
    where for<'a> <Self as TradeReduce>::TradeType: TryFromRef<BorrowedMessage<'a>>,
    <Self as TradeReduce>::ReductionType: std::fmt::Debug + ProcessTradeValue;
}

impl<T> CalcController for T
where
    T: Send + Sync + RiskProcessors + MktEventHandler + PublishResults + PortfolioSender,
    <T as TradeReduce>::TradeType: std::fmt::Debug
{
    fn start(
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
        mkt_params: MktMsgParams,
        pricing_options: &MarketPricingOptions,
    ) -> impl std::future::Future<Output=()> + Send
        where for<'a> <T as TradeReduce>::TradeType: TryFromRef<BorrowedMessage<'a>>,
            <T as TradeReduce>::ReductionType: std::fmt::Debug + ProcessTradeValue
    {
        async move {
	        let buffer_size = 10000;
            // 2 trade senders, 1 for current market, 1 for new market.
            let (pos_sender_curr, pos_recv_curr) = channel::<<T as TradeReduce>::ReductionType>(buffer_size);
            let (pos_sender_new, pos_recv_new) = channel::<<T as TradeReduce>::ReductionType>(buffer_size);
            // events about the new market event
            let (new_mkt_sender, new_mkt_receiver) = channel::<MarketType>(buffer_size);
            // new & current market portfolio
            let (curr_portfolio_sender, curr_portfolio_recv) =
                channel::<(PortfolioType, TradeRep<<T as TradeReduce>::ReductionType>)>(buffer_size);
            let (new_portfolio_sender, new_portfolio_recv) =
                channel::<(PortfolioType, TradeRep<<T as TradeReduce>::ReductionType>)>(buffer_size);
	        // whether to resend the whole portfolio to trade_processor_new
            let (resend_sender, resend_recv) = channel::<bool>(buffer_size);
	        // whether the portfolio was accepted by the trade_processor_curr
            let (accept_sender, accept_recv) = channel::<i32>(buffer_size);
	        let (fut_mkt_ready_s, fut_mkt_ready_r) = channel::<bool>(buffer_size);

            // threads fail if any of them can not be created.
            tokio_scoped::scope(
                |scope| {
                    info!("Spawning __construct_portfolio");
                    scope.spawn(
                        self.__construct_portfolio(
                            pos_sender_new,
                            pos_sender_curr,
                            resend_recv,
                            pos_topic,
                        )
                    );

                    info!("Spawning _handle_mkt_events");
                    scope.spawn(
                        self._handle_mkt_events(
	                        mkt_topic,
	                        mkt_params,
	                        new_mkt_sender,
	                        fut_mkt_ready_s,
	                    )
                    );

                    info!("Spawning _trade_processor_new.");
                    scope.spawn(
                        self._trade_processor_new(
                            new_mkt_receiver,
                            pos_recv_new,
                            new_portfolio_sender,
                            resend_sender,
                            accept_recv,
	                        fut_mkt_ready_r,
                            self.metric(),
                            pricing_options,
                        )
                    );

                    info!("Spawning _trade_processor_curr");
                    scope.spawn(
                        self._trade_processor_curr(
                            pos_recv_curr,
                            curr_portfolio_sender,
                            new_portfolio_recv,
                            self.metric(),
                            pricing_options,
                            accept_sender,
                        )
                    );

                    info!("Spawning _publish_results");
                    scope.spawn(
                        self._publish_results(
	                        curr_portfolio_recv,
	                        results_topic,
	                    )
                    );
                }
            );
        }
    }
}
