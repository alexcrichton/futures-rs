use core::mem::Pin;

use futures_core::{Async, Poll};
use futures_core::task;

/// A future which "fuses" a future once it's been resolved.
///
/// Normally futures can behave unpredictable once they're used after a future
/// has been resolved, but `Fuse` is always defined to return `Async::Pending`
/// from `poll` after it has resolved successfully or returned an error.
///
/// This is created by the `Async::fuse` method.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Fuse<A: Async> {
    future: Option<A>,
}

pub fn new<A: Async>(f: A) -> Fuse<A> {
    Fuse {
        future: Some(f),
    }
}

impl<A: Async> Async for Fuse<A> {
    type Output = A::Output;

    fn poll(mut self: Pin<Self>, cx: &mut task::Context) -> Poll<A::Output> {
        // safety: we use this &mut only for matching, not for movement
        let v = match unsafe { Pin::get_mut(&mut self) }.future {
            Some(ref mut fut) => {
                // safety: this re-pinned future will never move before being dropped
                match unsafe { Pin::new_unchecked(fut) }.poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(v) => v
                }
            }
            None => return Poll::Pending,
        };

        // safety: we use this &mut only for a replacement, which drops the future in place
        unsafe { Pin::get_mut(&mut self) }.future = None;
        Poll::Ready(v)
    }
}
