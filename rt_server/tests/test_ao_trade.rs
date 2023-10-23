// tests for AOTrade

use rt_server::{ao_trade::{BeforePosition, AfterPosition, Payload, AOTrade,}, pricer::MarketPricingOptions};
use rt_server::portfolio::PricingResults;
use rt_server::pricer::{PricingMetric, PriceTradeAsync};


async fn ao_trade_1() {

    let ao_trade = AOTrade {
        payload: Payload {
            op: "PV".to_owned(),
            after: AfterPosition {
                position_id: 1,
            },
            before: Some(
                BeforePosition {
                    position_id: 2,
                }
            ),
        }
    };

    let pricing_options = MarketPricingOptions {
        pricing_endpoint: "pv".to_owned(),
        pricing_server: "localhost:5010".to_owned(),
    };

    let res = ao_trade.price(&pricing_options).await;
    let res2 = ao_trade.value_by_metric(
        PricingMetric::PV,
        &pricing_options,
    ).await;

    assert_eq!(res, Some(4.));
    assert_eq!(res2, PricingResults::new(PricingMetric::PV));

}


fn test_ao_trade() {
    todo!()
}
