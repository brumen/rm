use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;


//#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
//pub enum CurrNewMarket {
//    Current,
//    New,
//}

// MarketRef is market reference, so that not the entire
// market but only the reference to that market is
// passed around.
// #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
// pub struct CurrNewMarket(pub String);

// impl fmt::Display for CurrNewMarket {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }

// impl Deref for CurrNewMarket {
//     type Target = String;

//     fn deref(&self) -> &Self::Target {
// 	&self.0
//     }
// }

// impl CurrNewMarket {

//     pub fn new(name: String) -> Self {
//         Self(name)
//     }

//     pub fn next_market(&self, mn: &AllMarkets) -> Option<Self> {
// 	mn.above_market(self)
//     }
// }
