//! Definition of the Empty combinator, a future that's never ready.

use core::mem::Pin;
use core::marker;

use futures_core::{Async, Poll};
use futures_core::task;

/// A future which is never resolved.
///
/// This future can be created with the `empty` function.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Empty<T> {
    _data: marker::PhantomData<T>,
}

/// Creates a future which never resolves, representing a computation that never
/// finishes.
///
/// The returned future will forever return `Async::Pending`.
pub fn empty<T>() -> Empty<T> {
    Empty { _data: marker::PhantomData }
}

impl<T> Async for Empty<T> {
    type Output = T;

    fn poll(self: Pin<Self>, _: &mut task::Context) -> Poll<T> {
        Poll::Pending
    }
}
