use crate::trades::ao_trade::AOTrade;
use crate::trades::letf_trade::LETFTrade;

use rt_server_derive::GeneratePrice;

#[derive(GeneratePrice)]
pub enum AllTrades {
    AOTrade(AOTrade),
    LETFTrade(LETFTrade),
}
