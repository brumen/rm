use rt_server::trade::LETFTrade;

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
