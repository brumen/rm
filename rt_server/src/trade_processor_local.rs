// Trait that implements the 2 market pricing
use log::{info, debug,};
use std::sync::mpsc::{Receiver, Sender,};
//use futures_channel::mpsc::{Receiver, Sender,};
use std::sync::{Arc,Mutex,};
use futures::executor;

use crate::trade::{
    TradeDirection,
    BaseTrade,
};
use crate::portfolio::PortfolioType;
use crate::market::{MarketType, CurrNewMarket, };
use crate::pricer::{PriceTrade, PriceTradeAsync, PricingMetric, MarketPricingOptions, };
use crate::trade::TradeRep;

use crate::portfolio::PricingResults;
use crate::trade_processor::{MarketSwitching, TradeMarketDiscovery,};

/// Implements functionality of
/// trade_processor_curr and trade_processor_new
/// TT: inherited from BasicValue, which is inherited from TradeAggregation
pub trait RiskProcessorsLocal<TT> : MarketSwitching + TradeMarketDiscovery<TT>
where
    TT:  PartialEq + std::fmt::Debug + Clone + BaseTrade + PriceTrade
{

    fn _process_trade(
        &self,
        trade: &TT,
        metric: PricingMetric,
        curr_new_mkt: CurrNewMarket,
    ) -> PricingResults {

        let trade_id = trade.id();
        let trade_direction = trade.direction();

        let market = match curr_new_mkt {
            CurrNewMarket::Current => self._curr_mkt(),
            CurrNewMarket::New => self._new_mkt(),
        };

        debug!("_trade_processor_curr: Processing trade {}, dir {:?}", trade_id, trade_direction);
        let trade_v = trade.value_by_metric(
            metric,
            &market.lock().unwrap(),
        );

        debug!("_trade_processor_curr: Trade value = {:?}", trade_v);
        match trade_direction {
            TradeDirection::Create => trade_v,
            TradeDirection::Delete => - trade_v,
            TradeDirection::Update => todo!(),
        }
    }

    fn _price_trades(
        &self,
        trades_to_price: &[&TT],
        metric: PricingMetric,
        curr_new_mkt: CurrNewMarket,
    ) -> PricingResults {

        // TODO: THIS CAN BE WRITTEN IN THE FORM OF COMBINATORS!!!

        let all_results = PricingResults::new(metric);

        for trade in trades_to_price {
            all_results += self._process_trade(trade, metric, curr_new_mkt)
        }

        all_results
    }

    fn _trade_processor_curr(
        &self,
        trade_receiver: Receiver<TT>,
        curr_portfolio_sender: Sender<PortfolioType>,
        new_portfolio_receiver: Receiver<(PortfolioType, usize)>,
        metric: PricingMetric,
    ) {
        let mut new_potential_portfolio : Option<(PortfolioType, usize)>;
        let mut all_trades = TradeRep::new();
        let mut nb_conseq_processed_trades : usize;  // number of trades which have been consequitively processed before refreshing to the new
        // market is switched.
        let max_number_trades = 20;  // TODO: FACTOR THIS OUT

        // compute the initial portfolio
        let _ = self._find_initial_trades(&trade_receiver, &mut all_trades);  // this updates all_trades
        let mut curr_portfolio = self._price_trades(
	        &all_trades.all_trades_ref()[..],
            metric,
            CurrNewMarket::Current,
	    ).aggregate();

        let _ = curr_portfolio_sender.send(curr_portfolio.clone());

        loop {

            // receive new trade to price on current market
            nb_conseq_processed_trades = 0;
	        while let Ok(trade) = trade_receiver.try_recv() {
                debug!("_trade_processor_curr: Received good trade {:?}", trade);
                if !all_trades.contains(&trade) {
                    let trade_id = trade.id();
                    let trade_direction = trade.direction();
                    all_trades.add_trade(trade.clone());

                    debug!("_trade_processor_curr: Processing trade {}, dir {:?}", trade_id, trade_direction);

                    let market = self._curr_mkt().lock().unwrap();  // TODO: CHECK HERE!!!
                    let trade_v = trade.value_by_metric  (metric, &market);
                    debug!("_trade_processor_curr: Trade value = {:?}", trade_v);
                    match trade_direction {
                        TradeDirection::Create => curr_portfolio += trade_v,
                        TradeDirection::Delete => curr_portfolio -= trade_v,
                        _ => {},
                    }

                    debug!("_trade_processor_curr: Curr portfolio = {:?}", curr_portfolio);

                    let _ = curr_portfolio_sender.send(curr_portfolio.clone());

                    nb_conseq_processed_trades += 1;
                    if nb_conseq_processed_trades > max_number_trades {
                        info!("_trade_processor_curr: Interrupting trade processing, reached max number of trades to process {}.", max_number_trades);
                        break;
                    }
                }
            }

            // receive new portfolio, replace current w/ new.
            new_potential_portfolio = None;
            while let Ok(new_portfolio) = new_portfolio_receiver.try_recv() {
                new_potential_portfolio = Some(new_portfolio);
            }

            if let Some((new_p, new_l)) = new_potential_portfolio {
                self._switch_markets();
                let all_l = all_trades.len();
		        let mut replace_curr_w_new = false;

                if new_l >= all_l {  // new processor is further ahead
                    info!("_trade_processor_curr: Switching curr_p <- new_p.");
                    curr_portfolio = new_p;
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
        new_portfolio_sender: Sender<(PortfolioType, usize)>,  // results are sent here
        metric: PricingMetric,
    ) {

	    let mut all_trades = TradeRep::<TT>::new();

        loop {

            // handling new trade event
            let _ = self._find_initial_trades(&new_trade_receiver, &mut all_trades);
            debug!("_trade_processor_new: Nb all trades: {}", all_trades.len());

	        // new_market_event also updates the new market
            let new_market_event = self._new_market_event(
		        &new_market_receiver,
	        );

            if new_market_event {
                info!("_trade_processor_new: Working on {} trades", all_trades.keys().len());
                let mut new_portfolio = self._price_trades(
		            &all_trades.all_trades_ref()[..],
		            metric,
		            CurrNewMarket::New,
		        ).aggregate();
                info!("_trade_processor_new: Finished processing bulk trades.");

		        // catch up any remaining trades
		        while let Ok(trade) = new_trade_receiver.try_recv() {
		            debug!("_trade_processor_new: Catching on remaining trades.");
                    let trade_direction = trade.direction();
                    info!("_trade_processor_new: Processing trade {}, dir {:?}", trade.id(), trade_direction);
                    let market = self._new_mkt().lock().unwrap();  // TODO: CHECK HERE CAUSE OF unwrap
                    let trade_v = trade.value_by_metric(
                        metric,
                        &market,
                    );
                    info!("_trade_processor_new: Finished processing trade");
                    match trade_direction {
			            TradeDirection::Create => {new_portfolio += trade_v;},
			            TradeDirection::Delete => {new_portfolio -= trade_v;},
			            _ => {},
                    }
                    // update all_trades and agg_trades.
                    all_trades.add_trade(trade.clone());
		        }

		        // decisions whether to publish the market or not.
                info!("_trade_processor_new: Sending new portfolio to be published ({} trades).", new_portfolio.keys().len());
                let _ = new_portfolio_sender.send((new_portfolio.clone(), all_trades.len()));
            }
        }
    }

}


//impl<T, TT> RiskProcessorsLocal<TT> for T
//where
//    T: MarketSwitching + TradeMarketDiscovery<TT>,  // + PriceMultipleTrades<TT>,
//    TT: BaseTrade + PartialEq + std::fmt::Debug + Clone + PriceTrade,
//{ }
