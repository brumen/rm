// Trade processor interaction between current and new market.

use log::info;
use std::sync::mpsc::{Receiver, Sender,};
use std::sync::{Arc, Mutex,};
use std::ops::AddAssign;

use crate::trade::BaseTrade;
use crate::portfolio::{PortfolioType, PricingResults, };
use crate::market::{MarketType, CurrNewMarket};
use crate::pricer::{
    PriceTrade,
    PriceTradeAsync,
    PricingMetric,
    MarketPricingOptions,
};
use crate::trade::{TradeRep, TradeReduce,};
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
        //curr_portfolio: Arc<Mutex<PortfolioType>>,
        curr_new_mkt: CurrNewMarket,
    ) -> PricingResults;
}


pub trait ProcessTradeAsync<TT>
where
    TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTradeAsync + BaseTrade + Send + Sync
{
    async fn _process_trade(
        &self,
        trade: &TT,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType;
}


/// Pricing engine for trades for remote pricing
pub trait RiskProcessors<TT> : TradeMarketDiscovery<TT>
where
    TT: PartialEq + std::fmt::Debug + Clone + BaseTrade + Send + Sync,
{
    //type TR: PartialEq + std::fmt::Debug;
    /// computes the metric of the existing trades in
    /// all_trades, on either the new or the current market
    /// and updates the current_portfolio
    fn _price_existing_trades(
        &self,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<Self::ReductionType>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
    ) -> PortfolioType;

    /// adds new trades on the trade_receiver to the
    /// all_trades, and prices the new trades that came on it.
    fn _price_new_trades(
        &self,
        curr_portfolio: &mut PortfolioType,
        trade_receiver: &Receiver<TT>,
        all_trades: Arc<Mutex<TradeRep<Self::ReductionType>>>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<PortfolioType>,
    );

    /// current trade processor, reads on
    /// trade_receiver, and new_portfolio_receiver,
    /// and updates the curr_portfolio_sender.
    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<TT>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<(PortfolioType, TradeRep<Self::ReductionType>)>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {
        let mut new_potential_portfolio : Option<(PortfolioType, TradeRep<Self::ReductionType>)>;
        let all_trades = Arc::new(Mutex::new(TradeRep::<Self::ReductionType>::new()));
        let mut nb_conseq_processed_trades : usize;  // number of trades which have been consequitively processed before refreshing to the new
        let curr_portfolio = Arc::new(Mutex::new(PortfolioType::new()));

        let mut curr_portfolio = self._price_existing_trades(
            &trade_receiver,
            all_trades.clone(),
            metric,
            pricing_options,
            CurrNewMarket::Current,
        );

        loop {

            // receive new trade to price on current market
            nb_conseq_processed_trades = 0;

            self._price_new_trades(
                &mut curr_portfolio,
                &trade_receiver,
                all_trades.clone(),
                metric,
                pricing_options,
                CurrNewMarket::Current,
                &curr_portfolio_sender,
            );

            // let _ = curr_portfolio_sender.send(curr_portfolio.clone());

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                info!("_trade_processor_curr: New portfolio!");
                new_potential_portfolio = Some(new_portfolio);
            }

            //let mut curr_portf = curr_portfolio.lock().unwrap();
            let mut all_trades_l = all_trades.lock().unwrap();

            if let Some((new_p, new_trades)) = new_potential_portfolio {
                self._switch_markets();
                let all_l = all_trades_l.len();
		        let mut replace_curr_w_new = false;

                let new_l = new_p.len();  // TODO: CHECK IF THIS IS TRUE
                if new_l >= all_l {  // new processor is further ahead
                    info!("_trade_processor_curr: Switching curr_p <- new_p.");
                    curr_portfolio = new_p;

                    //*all_trades_l += new_trades;
                    for (trade_id, trade_val) in new_trades.iter() {
                        //let l = (*trade_val).clone();
                        //let mut v = *all_trades_l;
                        (*all_trades_l).insert((*trade_id).clone(), (*trade_val).clone());
                        //*all_trades_l.insert((*trade_id).clone(), (*trade_val).clone());
                    }

		            replace_curr_w_new = true;
                } else if (new_l < all_l) && (new_l >= all_l - nb_conseq_processed_trades - 1) {  // new is not ahead, but we can still update.
                    info!("_trade_processor_curr: Extending the portfolio w/ new one");
                    curr_portfolio.extend(new_p.0.into_iter());
		            replace_curr_w_new = true;
                }
		        if replace_curr_w_new {
                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());
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
        new_portfolio_sender: Sender<(PortfolioType, TradeRep<Self::ReductionType>)>,  // results are sent here
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {

	    let all_trades = Arc::new(Mutex::new(TradeRep::<Self::ReductionType>::new()));

        loop {

	        // new_market_event also updates the new market
            let new_market_event = self._new_market_event(
		        &new_market_receiver,
	        );

            if new_market_event {

                let new_portfolio = self._price_existing_trades(
                    &new_trade_receiver,
                    all_trades.clone(),
                    metric,
                    pricing_options,
                    CurrNewMarket::New,
                );

                //let l = *(all_trades.lock().unwrap());
                info!("_trade_processor_new: Pricing finished!");
                let _ = new_portfolio_sender.send(
                    (new_portfolio, TradeRep((all_trades.lock().unwrap()).clone()))
                );
            }
        }
    }
}
