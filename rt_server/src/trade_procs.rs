// Trade processor interaction between current and new market.

use tracing::info;
use std::sync::mpsc::{Receiver, Sender,};
use std::sync::{Arc, Mutex,};

use crate::trade::BaseTrade;
use crate::portfolio::{PortfolioType, PricingResults, };
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
        trade: TT,
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
        all_trades: &mut TradeRep<Self::ReductionType>,
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
        all_trades: &mut TradeRep<Self::ReductionType>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
        curr_new_mkt: CurrNewMarket,
        new_trades_sender: &Sender<(PortfolioType, TradeRep<Self::ReductionType>)>,
    );

    /// current trade processor, reads on
    /// trade_receiver, and new_portfolio_receiver,
    /// and updates the curr_portfolio_sender.
    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<TT>,
        curr_portfolio_sender: Sender<(PortfolioType, TradeRep<Self::ReductionType>)>,
        new_portfolio_receiver: Receiver<(PortfolioType, TradeRep<Self::ReductionType>)>,
        metric: PricingMetric,
        pricing_options: &MarketPricingOptions,
    ) {
        let mut new_potential_portfolio : Option<(PortfolioType, TradeRep<Self::ReductionType>)>;
        let mut all_trades = TradeRep::<Self::ReductionType>::new();
        let curr_portfolio = Arc::new(Mutex::new(PortfolioType::new()));

        info!("Pricing existing trades on CURRENT market");
        let mut curr_portfolio = self._price_existing_trades(
            &trade_receiver,
            &mut all_trades,
            metric,
            pricing_options,
            CurrNewMarket::Current,
        );

        loop {

            self._price_new_trades(
                &mut curr_portfolio,
                &trade_receiver,
                &mut all_trades,
                metric,
                pricing_options,
                CurrNewMarket::Current,
                &curr_portfolio_sender,
            );

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                new_potential_portfolio = Some(new_portfolio);
            }

            if let Some((new_p, new_trades)) = new_potential_portfolio {
                self._switch_markets();

                let all_l = all_trades.len();
                let new_l = new_trades.len();
                info!("_trade_processor_curr: New trades: {} Curr trades: {}", new_l, all_l);
                if new_l >= all_l {  // new processor is further ahead
                    info!("_trade_processor_curr: Switching curr_p <- new_p: {}", new_trades.len());
                    curr_portfolio = new_p;
                    all_trades += &new_trades;
                    let _ = curr_portfolio_sender.send(
                        (curr_portfolio.clone(), TradeRep(all_trades.clone()))
                    );
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

	    let mut all_trades = TradeRep::<Self::ReductionType>::new();

        loop {

	        // new_market_event also updates the new market
            let new_market_event = self._new_market_event(
		        &new_market_receiver,
	        );

            if new_market_event {
                info!("Pricing existing trades on NEW market: {} trades", all_trades.len());

                // compute the existing trades on something fast, like spark
                let mut new_portfolio = self._price_existing_trades(
                    &new_trade_receiver,
                    &mut all_trades,
                    metric,
                    pricing_options,
                    CurrNewMarket::New,
                );

                let _ = new_portfolio_sender.send(
                    (new_portfolio, TradeRep(all_trades.clone()))
                );
            }
        }
    }
}
