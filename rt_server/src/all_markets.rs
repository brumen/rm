use dashmap::DashMap;
use std::ops::Deref;
use crate::market::MarketTypeT;

/// list of (market names, actual market)
// MT.. market type
// MP .. market params.
// MT = MarketTypeT<MP>
pub(crate) struct AllMarkets<MT>(DashMap<String, MT>);

impl<MT> Deref for AllMarkets<MT> {
    type Target = DashMap<String, MT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}




impl<MP, MT> AllMarkets<MT>
where
    MP: Clone,
    MT: MarketTypeT<MP=MP>
{
    //type MT = dyn MarketTypeT<MP=MP>;

    // creates a new empty all markets structure
    pub(crate) fn new() -> Self {
        Self(DashMap::<String,MT>::new())
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
        if self.0.len() == 0 {
            return None;
        }

        // we have at least one market.
        let market_elt = self.0.iter().nth(0)?;
        let mo = market_elt.value();

        Some(mo.market_params().clone())

    }

    // pub(crate) fn remove_market(&self, market_name: &String) {
    //     self.remove(&market_name)
        // if let Some(market_nb) = self._find_market(market_name) {

        // }  // otherwise dont do anything

    //}
}
