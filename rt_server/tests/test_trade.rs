use rt_server::markets::letf_market::{LETFMarketType, LETFMarketTypes};
use rt_server::pricer::PriceTrade;
use rt_server::trade::{BaseTrade, TradeDirection};
use rt_server::trades::trade_letf::LETFTrade;
use std::sync::Arc;

#[test]
fn test_letftrade() {
    let letf = LETFTrade {
        trade_id: "trade_1".to_string(),
        stock: "AAPL".to_string(),
        amount: 1.,
        beta: 3.,
        stock_value: Some(2.),
    };

    assert_eq!(letf.id(), "trade_1".to_string());
    assert_eq!(letf.direction(), TradeDirection::Create);
}

#[test]
fn test_pricing() {
    // tests if LETF trade prices correctly.
    let letf = LETFTrade {
        trade_id: "trade_1".to_string(),
        stock: "AAPL".to_string(),
        amount: 1.,
        beta: 3.,
        stock_value: Some(2.),
    };

    let l1 = Arc::new(LETFMarketType::new("l1".to_string()));
    l1.market
        .insert(LETFMarketTypes::Stock("AAPL".to_string()), 30.);

    let letf_price = letf.price(l1); // TODO: This should be awaited. This is wrong.
}
