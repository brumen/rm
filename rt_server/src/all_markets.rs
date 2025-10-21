use crate::market::{MarketTypeT};


/// list of (market names, actual market)
// MT.. market type
// MP .. market params.
// MT = MarketTypeT<MP>
#[derive(Debug)]
pub(crate) struct AllMarkets<MT>(Vec<MT>);

impl<MP> AllMarkets<dyn MarketTypeT<MP=MP>>
where
    dyn MarketTypeT<MP=MP> + 'static: Sized
{
    //type MT = dyn MarketTypeT<MP=MP>;

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

    pub(crate) fn get(&self, market_nb: usize) -> String {
	self.0[market_nb].market_name()
    }

    /// attempts to find the market name in the AllMarkets -
    /// if it cant find it, returns None
    fn _find_market(&self, mkt_name: &String) -> Option<usize> {
	self.0.iter().position(|r| r.market_name() == *mkt_name)
    }

    // gets the reference to the market w/ the name
    pub(crate) fn get_m(&self, market_name: String) -> Option<&dyn MarketTypeT<MP=MP>> {
        let market_nb = self._find_market(&market_name)?;

        Some(&self.0[market_nb])
    }

    /// finds the market above
    /// returns None if it's already the last market.
    pub(crate) fn above_market(&self, mkt_name: &String) -> Option<&dyn MarketTypeT<MP=MP>> {

	match self._find_market(mkt_name) {
	    None => None,
	    Some(found_mkt_nb) => {
		if found_mkt_nb == self.0.len() - 1 {
		    return None;
		}
		Some(&self.0[found_mkt_nb + 1])
	    }
	}
    }

    pub(crate) fn next_market(&self, market_name: String) -> Option<&dyn MarketTypeT<MP=MP>> {
	self.above_market(&market_name)
    }

    pub(crate) fn len(&self) -> usize {
	self.0.len()
    }
}
