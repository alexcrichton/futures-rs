#![feature(await_macro, async_await, futures_api)]
#![cfg(feature = "compat")]

use tokio::timer::Delay;
use tokio::runtime::Runtime;
use std::time::Instant;
use futures::prelude::*;
use futures::compat::Future01CompatExt;

#[test]
fn can_use_01_futures_in_a_03_future_running_on_a_01_executor() {
    let f = async {
        await!(Delay::new(Instant::now()).compat())
    };

    let mut runtime = Runtime::new().unwrap();
    runtime.block_on(f.boxed().compat()).unwrap();
}
