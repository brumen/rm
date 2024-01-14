use rt_server::trade::{BaseTrade, LETFTrade, TradeDirection};

use rt_server::market::MarketType;
use rt_server::pricer::PriceTrade;

#[test]
fn test_trade_processors_1() {
    let letf = LETFTrade {
        trade_id: "trade_1".to_string(),
        stock: "AAPL".to_string(),
        amount: 1.,
        beta: 3.,
        stock_value: Some(2.),
    };
}
