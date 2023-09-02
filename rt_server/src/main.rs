// Starts the controller.

// Windows usage:
// ADD THIS TO POWERSHELL:
// $env:OPENSSL_DIR = 'C:\Tools\vcpkg\installed\x64-windows-static'
// $env:OPENSSL_STATIC = 'Yes'

//use std::env::args;

#![feature(async_fn_in_trait)]

mod trade;
mod ao_trade;
mod encdec;
mod portfolio;
mod pricer;
mod market;
mod ref_deref;
mod controller;
mod trader;
mod publish;
mod streaming;
mod trade_processor;

mod rm_local;
mod engine;
mod mkt_handler;
mod portfolio_sender;
mod letf_trader;
mod ao_risk;


fn main() {

    env_logger::init();  // TODO: CHECK IF THIS NEEDS TO BE DONE!!!

    letf_trader::main_letf_trader();
    // ao_risk::ao_main_risk();
}
