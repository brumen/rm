use ractor::async_trait;
use rdkafka::message::BorrowedMessage;
use thiserror::Error;

// MP is mnemonic for market parameters.
#[async_trait]
pub trait MarketTypeT {
    type MP;

    fn new(market_name: String, mp: Self::MP) -> Box<dyn MarketTypeT<MP=Self::MP> + Send + Sync> where Self: Sized;
    fn market_name(&self) -> String;
    async fn get(&self, stock: &String) -> Option<f64>;  // getting stock values.
    async fn insert(&self, key: String, value: f64);  // Important: insert is _NOT_ mutable self
    fn is_empty(&self) -> bool;
    fn try_from_ref(market_name: String, value: &BorrowedMessage, mp: Self::MP) -> Result<Box<dyn MarketTypeT<MP=Self::MP> + Send + Sync>, MarketTypeError> where Self:Sized;
    fn market_params(&self) -> &Self::MP;
}


#[derive(Error, Debug)]
pub enum MarketTypeError {
    #[error("General Market Error")]
    GeneralError(String),
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to MarketType")]
    CantConvertToMarket(#[from] serde_json::Error),
}
