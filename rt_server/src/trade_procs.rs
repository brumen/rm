// Trade processor interaction between current and new market.

use log::info;
use std::sync::mpsc::{Receiver, Sender,};
use std::sync::{Arc, Mutex,};

use crate::trade::BaseTrade;
use crate::portfolio::PortfolioType;
use crate::market::{MarketType, CurrNewMarket};
use crate::pricer::{
    PriceTrade,
    PriceTradeAsync,
    PricingMetric,
    MarketPricingOptions,
};
use crate::trade::TradeRep;
use crate::trade_processor::TradeMarketDiscovery;


pub trait ProcessTradeSync<TT>
where
    TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTrade + BaseTrade + std::marker::Send
{
    fn _process_trade(
        &self,
        trade: &TT,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        curr_new_mkt: CurrNewMarket,
    );
}


pub trait ProcessTradeAsync<TT>
where
    TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send
{
    async fn _process_trade(
        &self,
        trade: TT,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    );
}


/// Pricing engine for trades for remote pricing
pub trait RiskProcessors<TT> : TradeMarketDiscovery<TT>
where
    TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade + BaseTrade + Send + Sync
{

    /// run computations reads on the trade receiver, computes the value of the
    ///   trades (metric) and updates the all_trades.
    ///   It also posts on the sender.
    fn _run_computations(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    );

    /// computes the metric of the existing trades in
    /// all_trades, on either the new or the current market
    /// and updates the current_portfolio
    fn _existing_trades(
        &self,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    );

    fn _new_trades(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<TT>>>,
        curr_portfolio: Arc<Mutex<PortfolioType>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_portfolio_sender: &Sender<PortfolioType>,
        curr_new_mkt: CurrNewMarket,
    );

    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<TT>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<PortfolioType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {
        let mut new_potential_portfolio : Option<PortfolioType>;
        let all_trades = Arc::new(Mutex::new(TradeRep::new()));
        let mut nb_conseq_processed_trades : usize;  // number of trades which have been consequitively processed before refreshing to the new
        let curr_portfolio = Arc::new(Mutex::new(PortfolioType::new()));

        self._existing_trades(
            all_trades.clone(),
            curr_portfolio.clone(),
            metric,
            pricing_options,
            &curr_portfolio_sender,
            CurrNewMarket::Current,
        );

        loop {

            // receive new trade to price on current market
            nb_conseq_processed_trades = 0;

            self._new_trades(
                &trade_receiver,
                all_trades.clone(),
                curr_portfolio.clone(),
                metric,
                pricing_options,
                &curr_portfolio_sender,
                CurrNewMarket::Current,
            );

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                info!("_trade_processor_curr: New portfolio!");
                new_potential_portfolio = Some(new_portfolio);
            }

            let mut curr_portf = curr_portfolio.lock().unwrap();
            let all_trades_l = all_trades.lock().unwrap();

            if let Some(new_p) = new_potential_portfolio {
                self._switch_markets();
                let all_l = all_trades_l.len();
		        let mut replace_curr_w_new = false;

                let new_l = new_p.len();  // TODO: CHECK IF THIS IS TRUE
                if new_l >= all_l {  // new processor is further ahead
                    info!("_trade_processor_curr: Switching curr_p <- new_p.");
                    *curr_portf = new_p;
		            replace_curr_w_new = true;
                } else if (new_l < all_l) && (new_l >= all_l - nb_conseq_processed_trades - 1) {  // new is not ahead, but we can still update.
                    info!("_trade_processor_curr: Extending the portfolio w/ new one");
                    curr_portf.extend(new_p.0.into_iter());
		            replace_curr_w_new = true;
                }
		        if replace_curr_w_new {
                    let _ = curr_portfolio_sender.send(curr_portf.clone());
		        }
            }
        }
    }

    /// processes the trades on the new market.
    /// new_market_receiver:
    fn _trade_processor_new(
        &self,
        new_market_receiver: Receiver<MarketType>,
        new_trade_receiver: Receiver<TT>,  // receiving new additional trades
        new_portfolio_sender: Sender<PortfolioType>,  // results are sent here
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {

	    let all_trades = Arc::new(Mutex::new(TradeRep::<TT>::new()));

        loop {

	        // new_market_event also updates the new market
            let new_market_event = self._new_market_event(
		        &new_market_receiver,
	        );

            if new_market_event {
                info!("_trade_processor_new: New market event: Working on {} trades.", all_trades.lock().unwrap().keys().len());

                let new_portfolio = Arc::new(Mutex::new(PortfolioType::new()));
                self._run_computations(
                    &new_trade_receiver,
                    all_trades.clone(),
                    new_portfolio.clone(),
                    metric,
                    pricing_options,
                    &new_portfolio_sender,
                    CurrNewMarket::New,
                );

		        // decisions whether to publish the market or not.
                //info!("_trade_processor_new: Sending new portfolio to be published ({} trades).", new_portfolio.keys().len());
                //let _ = new_portfolio_sender.send(new_portfolio.clone());
            }
        }
    }
}
