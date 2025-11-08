use dashmap::DashMap;
use std::ops::Deref;
use std::sync::Arc;

use crate::market::MarketTypeT;

/// list of (market names, actual market)
// MT.. market type
// MP .. market params.
// MT = MarketTypeT<MP>
pub(crate) struct AllMarkets<MT> {
    pub(crate) markets: DashMap<String, MT>,
    pub(crate) market_names: Vec<String>,
}

// impl<MT> Deref for AllMarkets<MT> {
//     type Target = DashMap<String, MT>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }




impl<MP, MT> AllMarkets<MT>
where
    MP: Clone,
    MT: MarketTypeT<MP=MP> + Clone,  // this will be fine since MT is an Arc.
{
    //type MT = Arc<dyn MarketTypeT<MP=MP> + Sync + Send>;

    // creates a new empty all markets structure
    pub(crate) fn new() -> Self {
        Self {
            markets: DashMap::<String,MT>::new(),
            market_names: Vec::<String>::new(),
        }
    }

    pub(crate) fn get(&self, market_name: &String) -> Option<MT> {
        let mv = self.markets.get(market_name)?;
        let a1 = mv.value().clone();
        Some(a1)
    }

    // inserts the market into the all structure.
    pub(crate) fn insert(&self, market_name: String, market: MT) {
        self.markets.insert(market_name, market);
    }

    pub(crate) fn get_market(&self, market_nb: usize) -> Option<&String> {
        self.market_names.get(market_nb)
    }

    // pub(crate) fn get(&self, market_name: &String) -> &MT {
    //     let l = self.0.get(market_name).unwrap();

    //     l.value()
    //     // let k = l.value();

    //     // k
    // }

    /// Default implemnentation of the market names.
    // pub(crate) fn new(nb_middle: usize) -> Self {
    //     let mut middle_markets = vec![];
    //     middle_markets.push(MT::new("current".to_string()));
    //     for middle_nb in 0..nb_middle {
    //         middle_markets.push(
    //     	MT::new(format!("new_{middle_nb}"))
    //         );
    //     }
    //     middle_markets.push(MT::new("new".to_string()));

    //     Self(middle_markets)
    // }


    /// attempts to find the market name in the AllMarkets -
    /// if it cant find it, returns None
    // fn _find_market(&self, mkt_name: &String) -> Option<usize> {
    //     self.0.iter().position(|r| r.market_name() == *mkt_name)
    // }

    // gets the reference to the market w/ the name
    // pub(crate) fn get_m(&self, market_name: &String) -> Option<MT> {
    //     let k = self.get(market_name)?;

    //     let m = k.value();

    //     Some(m)
    //     //market_ref.value()
    //     // Some(&self.0[market_nb])
    // }

    /// finds the market above
    /// returns None if it's already the last market.
    // pub(crate) fn above_market(&self, mkt_name: &String) -> Option<&dyn MarketTypeT<MP=MP>> {

    //     match self._find_market(mkt_name) {
    //         None => None,
    //         Some(found_mkt_nb) => {
    //     	if found_mkt_nb == self.0.len() - 1 {
    //     	    return None;
    //     	}
    //     	Some(&self.0[found_mkt_nb + 1])
    //         }
    //     }
    // }

    // pub(crate) fn next_market(&self, market_name: String) -> Option<&dyn MarketTypeT<MP=MP>> {
    //     self.above_market(&market_name)
    //}

    /// returns the market params of some market in the collection
    pub(crate) fn get_market_params(&self) -> Option<MP> {
        //
        if self.market_names.len() == 0 {
            return None;
        }

        // we have at least one market.
        let market_elt = self.markets.iter().nth(0)?;
        let mo = market_elt.value();

        Some(mo.market_params().clone())

    }

    pub(crate) fn remove(&self, market_name: &String) {
        self.markets.remove(market_name);
    }
        //     self.remove(&market_name)
        // if let Some(market_nb) = self._find_market(market_name) {

        // }  // otherwise dont do anything

    //}
}
