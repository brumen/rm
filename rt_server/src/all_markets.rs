use dashmap::DashMap;

use crate::market::MarketTypeT;

/// list of (market names, actual market)
// MT.. market type
// MP .. market params.
// MT = MarketTypeT<MP>
pub(crate) struct AllMarkets<MT> {
    pub(crate) markets: DashMap<String, MT>,
    pub(crate) market_names: DashMap<usize, String>,  // mapping of numbers to markets.
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
            market_names: DashMap::<usize, String>::new(),
        }
    }

    pub(crate) fn get(&self, market_name: &String) -> Option<MT> {
        let actual_market = self.markets.get(market_name)?;
        Some(actual_market.value().clone())  // .clone here is OK, since we're using it on Arc (MT = Arc<...>)
    }

    // inserts the market into the all structure.
    pub(crate) fn insert(&self, market_name: String, market: MT) {
        self.markets.insert(market_name, market);
    }

    pub(crate) fn insert_name(&self, market_name: String, market_nb: usize) {
        self.market_names.insert(market_nb, market_name);
    }

    pub(crate) fn get_market(&self, market_nb: &usize) -> Option<String> {
        let mn = self.market_names.get(market_nb)?;
        Some(mn.value().clone())
    }

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

    // remove the market from self.markets
    pub(crate) fn remove(&self, market_name: &String) {
        self.markets.remove(market_name);
    }

    // remove the name from self.market_names
    pub(crate) fn remove_name(&self, market_name: &String) {
        // iterate through it and remove the name
        for market_nb_mn in self.market_names.iter() {
            let mn = market_nb_mn.value();
            let market_nb = market_nb_mn.key();
            if mn == market_name {
                self.market_names.remove(market_nb);
            }
        }
    }

    // returns the market name corresponding to the largets number in self.market_names
    pub(crate) fn last_market_name(&self) -> String {
        let mut highest_mkt: usize = 0;
        let mut highest_mkt_name: String = String::new();
        for mkt_nb_name in self.market_names.iter() {
            let mkt_nb = mkt_nb_name.key();
            if *mkt_nb >= highest_mkt {
                highest_mkt = *mkt_nb;
                highest_mkt_name = mkt_nb_name.value().to_string();
            }
        }
        highest_mkt_name
    }
}
