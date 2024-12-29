use rt_server::portfolio::PortfolioType;
use tokio;
use rt_server::pricer::{Decoder, MarketPricingOptions, RestPricerSpark, PricingMetric, };
use rt_server::market::CurrNewMarket;
use rt_server::ao_trade::{
    AOTrade, Payload, AfterPosition, BeforePosition
};
use rt_server::trade::TradeRep;


fn sample_ao_trade() -> AOTrade {

    let ao_trade = AOTrade {
        payload: Payload {
            op: "PV".to_owned(),
            after: AfterPosition { position_id: 42651 },
            before: None,
        },
    };

    ao_trade
}

struct SampleController;

impl Decoder for SampleController {}

impl RestPricerSpark<AOTrade> for SampleController {

    fn _pricing_endpoint_spark(&self, market_: CurrNewMarket, metric: PricingMetric) -> String {
	"pv/".to_string()
    }

    fn _pricing_server_spark(&self) -> String {
	"192.168.1.107:8000".to_string()
    }
}


#[tokio::test]
async fn test_sample_controller() {
    let sc = SampleController;
    let ao_trade = sample_ao_trade();
    let mut ao_tr = TradeRep::default();
    ao_tr += &ao_trade;
    let mpo = MarketPricingOptions {
	pricing_server: "192.168.1.107:8000".to_string(),
	pricing_endpoint: "pv/".to_string(),
    };
    
    let res = sc.price_trades_on_spark(&ao_tr, PricingMetric::PV, &mpo, CurrNewMarket::Current).await;

    assert_eq!(res.contains_key("42651"), true);    
}
