use std::collections::HashMap;
use std::sync::mpsc::channel;
use std::thread;

use crate::market::MktMsgParams;
use crate::mkt_handler::MktEventHandler;
use crate::portfolio::{PortfolioType, PortfolioSender, };


use crate::market::MarketType;

use crate::publish::PublishResults;
use crate::trade_processor::RiskProcessors;

pub type PricingParams = HashMap<String, f64>;

use crate::trade::TradeAggregation;

pub trait CalcController {
    fn start (
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
        mkt_params: MktMsgParams,
    );
}

///
/// main function that starts the various threads.
///
impl<T> CalcController for T
where
    T: Send + Sync + TradeAggregation + RiskProcessors + MktEventHandler + PublishResults + PortfolioSender,
{

    fn start (
        &self,
        pos_topic: String,     // position topic on kafka
        mkt_topic: String,     // market topic
        results_topic: String, // publish the results topic
        mkt_params: MktMsgParams,
    ) {
        // 2 trade senders, 1 for current market, 1 for new market.
        let (pos_sender_curr, pos_recv_curr) = channel::<T::TT>();
        let (pos_sender_new, pos_recv_new) = channel::<T::TT>();
        // events about the new market event
        let (new_mkt_sender, new_mkt_receiver) = channel::<MarketType>();
        // new & current market portfolio
        let (curr_portfolio_sender, curr_portfolio_recv) = channel::<PortfolioType>();
        let (new_portfolio_sender, new_portfolio_recv) = channel::<(PortfolioType, Vec<T::TT>)>();

        // threads fail if any of them can not be created.
        thread::scope(|s| {
            let _ = thread::Builder::new()
                .name("accepting_trades".to_string())
                .spawn_scoped(s, move || {
                    self.__construct_portfolio(pos_sender_new, pos_sender_curr, pos_topic);
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("market_events".to_string())
                .spawn_scoped(s, move || {
                    self._handle_mkt_events(
                        mkt_topic,
                        mkt_params,
                        new_mkt_sender,
                    )  // MktMsgParams::AOParams(AOStruct{mkt_sender: new_mkt_sender}))
                        // LETF
                    // MktMsgParams::LETFParams(
                        //     LETFP {
                        //         curr_mkt: Arc::clone(&self.curr_market),
                        //         new_mkt_sender,
                        //     }
                        // )

                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("new_portfolio".to_string())
                .spawn_scoped(s, move || {
                    self._trade_processor_new(
                        new_mkt_receiver,
                        pos_recv_new,
                        new_portfolio_sender,
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
                    );
                })
                .unwrap();

            let _ = thread::Builder::new()
                .name("publish_thread".to_string())
                .spawn_scoped(s, move || {
                    self._publish_results(curr_portfolio_recv, results_topic);
                })
                .unwrap();
        });
    }
}
