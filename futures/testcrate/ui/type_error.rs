#![feature(async_await, futures_api, generators)]

use futures::*;

#[async_stream]
fn foo() -> i32 {
    let a: i32 = "a"; //~ ERROR: mismatched types
    stream_yield!(1);
}

fn main() {}
