use ractor::async_trait;
use rdkafka::message::BorrowedMessage;
use thiserror::Error;
use std::sync::Arc;

// MP is mnemonic for market parameters.
#[async_trait]
pub trait MarketTypeT
where
    Self: Send + Sync
{
    type MP;

    fn new(market_name: String, mp: Self::MP) -> Arc<Self> where Self: Sized;
    fn market_name(&self) -> String;
    async fn get(&self, stock: &String) -> Option<f64>;  // getting stock values.
    async fn insert(&self, key: String, value: f64);  // Important: insert is _NOT_ mutable self
    fn is_empty(&self) -> bool;
    fn try_from_ref(market_name: String, value: &BorrowedMessage, mp: Self::MP) -> Result<Arc<Self>, MarketTypeError> where Self:Sized;
    fn market_params(&self) -> Self::MP;
}


#[async_trait]
impl<T:MarketTypeT> MarketTypeT for Arc<T> {
    type MP = T::MP;

    fn new(market_name: String, mp: Self::MP) -> Arc<Self> where Self: Sized {
        Arc::new(T::new(market_name, mp))
    }

    fn market_name(&self) -> String {
        (**self).market_name()
    }

    async fn get(&self, stock: &String) -> Option<f64> {
        self.get(stock).await
    }

    async fn insert(&self, key: String, value: f64) {
        self.insert(key, value).await;
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn try_from_ref(market_name: String, value: &BorrowedMessage, mp: Self::MP) -> Result<Arc<Self>, MarketTypeError> where Self:Sized {
        Ok(Arc::new(T::try_from_ref(market_name, value, mp)?))
    }

    fn market_params(&self) -> Self::MP {
        (**self).market_params()
    }

}


// MP is mnemonic for market parameters.
#[async_trait]
pub trait MarketTypeTOriginal {
    type MP;

    fn new(market_name: String, mp: Self::MP) -> Arc<dyn MarketTypeTOriginal<MP=Self::MP> + Send + Sync> where Self: Sized + Send + Sync;
    fn market_name(&self) -> String;
    async fn get(&self, stock: &String) -> Option<f64>;  // getting stock values.
    async fn insert(&self, key: String, value: f64);  // Important: insert is _NOT_ mutable self
    fn is_empty(&self) -> bool;
    fn try_from_ref(market_name: String, value: &BorrowedMessage, mp: Self::MP) -> Result<Arc<dyn MarketTypeTOriginal<MP=Self::MP> + Send + Sync>, MarketTypeError> where Self:Sized + Send + Sync;
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
