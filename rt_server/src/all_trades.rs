use crate::ao_trade::AOTrade;
use crate::trade::LETFTrade;

use rt_server_derive::GeneratePrice;


#[derive(GeneratePrice)]
pub enum AllTrades {
    AOTrade(AOTrade),
    LETFTrade(LETFTrade),
}
