use futures_core::{Poll, Stream};
use futures_core::task;
use futures_sink::{Sink};

use core::marker::Unpin;
use core::mem::PinMut;

/// Sink for the `Sink::sink_map_err` combinator.
#[derive(Debug)]
#[must_use = "sinks do nothing unless polled"]
pub struct SinkMapErr<S, F> {
    sink: S,
    f: Option<F>,
}

pub fn new<S, F>(s: S, f: F) -> SinkMapErr<S, F> {
    SinkMapErr { sink: s, f: Some(f) }
}

impl<S: Unpin, F> Unpin for SinkMapErr<S, F> {}

impl<S, F> SinkMapErr<S, F> {
    unsafe_pinned!(sink -> S);
    unsafe_unpinned!(f -> Option<F>);

    /// Get a shared reference to the inner sink.
    pub fn get_ref(&self) -> &S {
        &self.sink
    }

    /// Get a mutable reference to the inner sink.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    /// Get a pinned reference to the inner sink.
    pub fn get_pinned_mut<'a>(self: PinMut<'a, Self>) -> PinMut<'a, S> {
        unsafe { PinMut::new_unchecked(&mut PinMut::get_mut(self).sink) }
    }

    /// Consumes this combinator, returning the underlying sink.
    ///
    /// Note that this may discard intermediate state of this combinator, so
    /// care should be taken to avoid losing resources when this is called.
    pub fn into_inner(self) -> S {
        self.sink
    }

    fn expect_f(mut self: PinMut<Self>) -> F {
        self.f().take().expect("polled MapErr after completion")
    }
}

impl<S, F, E> Sink for SinkMapErr<S, F>
    where S: Sink,
          F: FnOnce(S::SinkError) -> E,
{
    type SinkItem = S::SinkItem;
    type SinkError = E;

    fn poll_ready(mut self: PinMut<Self>, cx: &mut task::Context) -> Poll<Result<(), Self::SinkError>> {
        self.sink().poll_ready(cx).map_err(|e| self.expect_f()(e))
    }

    fn start_send(mut self: PinMut<Self>, item: Self::SinkItem) -> Result<(), Self::SinkError> {
        self.sink().start_send(item).map_err(|e| self.expect_f()(e))
    }

    fn poll_flush(mut self: PinMut<Self>, cx: &mut task::Context) -> Poll<Result<(), Self::SinkError>> {
        self.sink().poll_flush(cx).map_err(|e| self.expect_f()(e))
    }

    fn poll_close(mut self: PinMut<Self>, cx: &mut task::Context) -> Poll<Result<(), Self::SinkError>> {
        self.sink().poll_close(cx).map_err(|e| self.expect_f()(e))
    }
}

impl<S: Stream, F> Stream for SinkMapErr<S, F> {
    type Item = S::Item;

    fn poll_next(mut self: PinMut<Self>, cx: &mut task::Context) -> Poll<Option<S::Item>> {
        self.sink().poll_next(cx)
    }
}
