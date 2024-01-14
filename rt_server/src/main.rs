// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'

//use std::env::args;

//#![feature(async_fn_in_trait)]

mod ao_risk;
mod ao_trade;
mod controller;
mod encdec;
mod engine;
mod letf_trader;
mod market;
mod mkt_handler;
mod portfolio;
mod portfolio_sender;
mod pricer;
mod publish;
mod ref_deref;
mod rm_local;
mod streaming;
mod trade;
mod trade_procs;
mod trader;

fn main() {
    env_logger::init(); // TODO: CHECK IF THIS NEEDS TO BE DONE!!!

    // letf_trader::main_letf_trader();
    ao_risk::ao_main_risk();
}
