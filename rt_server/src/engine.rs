use std::sync::mpsc::channel;
use std::thread;

use crate::market::MarketType;
use crate::market::MktMsgParams;
use crate::mkt_handler::MktEventHandler;
use crate::portfolio::PortfolioType;
use crate::portfolio_sender::PortfolioSender;
use crate::pricer::MarketPricingOptions;
use crate::publish::PublishResults;
use crate::trade::{BaseTrade, TradeRep,};
use crate::trade_procs::RiskProcessors;
use crate::pricer::PriceTradeAsync;


pub trait CalcController {
    //type TR;

    fn start(
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
        mkt_params: MktMsgParams,
        pricing_options: &MarketPricingOptions,
    );
}


impl<T> CalcController for T
where
    T: Send + Sync + RiskProcessors + MktEventHandler + PublishResults + PortfolioSender,
    //TT: Clone + Send + BaseTrade + PartialEq + std::fmt::Debug + PriceTradeAsync + Sync,

{
    //type TR = <T as RiskProcessors>::TR;
    //type TRR = <T as RiskProcessors>::TR;

    fn start(
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
        mkt_params: MktMsgParams,
        pricing_options: &MarketPricingOptions,
    ) {

        // 2 trade senders, 1 for current market, 1 for new market.
        let (pos_sender_curr, pos_recv_curr) = channel::<<T as PortfolioSender>::TR>();
        let (pos_sender_new, pos_recv_new) = channel::<<T as PortfolioSender>::TR>();
        // events about the new market event
        let (new_mkt_sender, new_mkt_receiver) = channel::<MarketType>();
        // new & current market portfolio
        let (curr_portfolio_sender, curr_portfolio_recv) = channel::<(PortfolioType, TradeRep<<T as PortfolioSender>::TR>)>();
        let (new_portfolio_sender, new_portfolio_recv) = channel::<(PortfolioType, TradeRep<<T as PortfolioSender>::TR>)>();
        let (resend_sender, resend_recv) = channel::<bool>();

        // threads fail if any of them can not be created.
        thread::scope(|s| {
            let _ = thread::Builder::new()
                .name("accepting_trades".to_string())
                .spawn_scoped(s, move || {
                    self.__construct_portfolio(
                        pos_sender_new,
                        pos_sender_curr,
                        resend_recv,
                        pos_topic,
                    );
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("market_events".to_string())
                .spawn_scoped(s, move || {
                    self._handle_mkt_events(mkt_topic, mkt_params, new_mkt_sender)
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("new_portfolio".to_string())
                .spawn_scoped(s, move || {
                    self._trade_processor_new(
                        new_mkt_receiver,
                        pos_recv_new,
                        new_portfolio_sender,
                        resend_sender,
                        self.metric(),
                        pricing_options,
                    );
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("curr_portfolio".to_string())
                .spawn_scoped(s, move || {
                    self._trade_processor_curr(
                        pos_recv_curr,
                        curr_portfolio_sender,
                        new_portfolio_recv,
                        self.metric(),
                        pricing_options,
                    );
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("publish_thread".to_string())
                .spawn_scoped(
                    s,
                    move || {
                        self._publish_results(
                            curr_portfolio_recv,
                            results_topic
                        );
                    })
                .unwrap();
        });
    }
}
