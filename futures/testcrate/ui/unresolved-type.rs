#![feature(async_await, futures_api, generators)]

use futures::*;

#[async_stream]
fn foo() -> Left {}

fn main() {}
