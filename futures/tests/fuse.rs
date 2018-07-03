#![feature(pin, futures_api)]

extern crate futures;

use std::mem::PinMut;
use futures::prelude::*;
use futures::future::ready;

mod support;

#[test]
fn fuse() {
    let mut future = ready::<i32>(2).fuse();
    support::panic_waker_cx(|cx| {
        assert!(PinMut::new(&mut future).poll(cx).is_ready());
        assert!(PinMut::new(&mut future).poll(cx).is_pending());
    })
}
