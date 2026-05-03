use ractor::async_trait;
use rdkafka::message::BorrowedMessage;
use std::sync::Arc;
use thiserror::Error;

// MP is mnemonic for market parameters.
// MK is mnemonic for market keys
#[async_trait]
pub trait MarketTypeT
where
    Self: Send + Sync,
    Self::MK: Send + Sync,
{
    type MP;
    type MK; // this has to be hashable, maybe some other stuff as well.

    fn new(market_name: String, mp: Self::MP) -> Arc<Self>
    where
        Self: Sized;
    fn market_name(&self) -> String;
    async fn get(&self, stock: &Self::MK) -> Option<f64>; // getting stock values.
    async fn insert(&self, key: Self::MK, value: f64); // Important: insert is _NOT_ mutable self
    fn is_empty(&self) -> bool;
    fn try_from_ref(
        market_name: String,
        value: &BorrowedMessage,
        mp: Self::MP,
    ) -> Result<Arc<Self>, MarketTypeError>
    where
        Self: Sized;
    fn market_params(&self) -> Self::MP;
    fn is_used(&self) -> bool {
        true
    }
}

// Setting the name of the market
pub trait SetName {
    fn set_name(&mut self, new_name: String);
}

#[async_trait]
impl<T: MarketTypeT> MarketTypeT for Arc<T> {
    type MP = T::MP;
    type MK = T::MK;

    fn new(market_name: String, mp: Self::MP) -> Arc<Self>
    where
        Self: Sized,
    {
        Arc::new(T::new(market_name, mp))
    }

    fn market_name(&self) -> String {
        (**self).market_name()
    }

    async fn get(&self, stock: &Self::MK) -> Option<f64> {
        (**self).get(stock).await
    }

    async fn insert(&self, key: Self::MK, value: f64) {
        (**self).insert(key, value).await;
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn try_from_ref(
        market_name: String,
        value: &BorrowedMessage,
        mp: Self::MP,
    ) -> Result<Arc<Self>, MarketTypeError>
    where
        Self: Sized,
    {
        Ok(Arc::new(T::try_from_ref(market_name, value, mp)?))
    }

    fn market_params(&self) -> Self::MP {
        (**self).market_params()
    }

    fn is_used(&self) -> bool {
        Arc::strong_count(self) > 1
    }
}

// TODO: TO BE IMPLEMENTED AS SOON AS WE HAVE .market exposed on the trait.
// impl<T: MarketTypeT> PartialEq for T {
//     fn eq(&self, other: &Self) -> bool {
//         if self.market_name != other.market_name {
//             return false;
//         }

//         // TODO: HERE HAS TO CHANGE
//         for self_entry in self.market.iter() {
//             let self_key = self_entry.key();
//             if !other.market.contains_key(self_key) {
//                 return false;
//             }
//         }

//         for other_entry in other.market.iter() {
//             let other_key = other_entry.key();
//             if !self.market.contains_key(other_key) {
//                 return false;
//             }
//         }

//         true
//     }
// }

#[derive(Error, Debug)]
pub enum MarketTypeError {
    #[error("General Market Error")]
    GeneralError(String),
    #[error("Cant convert from utf messsage")]
    CantConvertUtf8(#[from] std::str::Utf8Error),
    #[error("Cant convert to MarketType")]
    CantConvertToMarket(#[from] serde_json::Error),
}
