use rt_server::trade::{BaseTrade, LETFTrade, TradeDirection};

use rt_server::market::MarketType;
use rt_server::pricer::PriceTrade;

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

    let market = MarketType::from([("AAPL".to_string(), 30.)]);

    let letf_price = letf.price(&market);
}
