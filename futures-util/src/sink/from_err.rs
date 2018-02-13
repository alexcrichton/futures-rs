use core::marker::PhantomData;

use futures_core::{Stream, Poll};
use futures_core::task;
use futures_sink::{Sink, StartSend};

/// A sink combinator to change the error type of a sink.
///
/// This is created by the `Sink::from_err` method.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct SinkFromErr<S, E> {
    sink: S,
    f: PhantomData<E>
}

pub fn new<S, E>(sink: S) -> SinkFromErr<S, E>
    where S: Sink
{
    SinkFromErr {
        sink: sink,
        f: PhantomData
    }
}

impl<S, E> SinkFromErr<S, E> {
    /// Get a shared reference to the inner sink.
    pub fn get_ref(&self) -> &S {
        &self.sink
    }

    /// Get a mutable reference to the inner sink.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.sink
    }

    /// Consumes this combinator, returning the underlying sink.
    ///
    /// Note that this may discard intermediate state of this combinator, so
    /// care should be taken to avoid losing resources when this is called.
    pub fn into_inner(self) -> S {
        self.sink
    }
}

impl<S, E> Sink for SinkFromErr<S, E>
    where S: Sink,
          E: From<S::SinkError>
{
    type SinkItem = S::SinkItem;
    type SinkError = E;

    fn start_send(&mut self, ctx: &mut task::Context, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.sink.start_send(ctx, item).map_err(|e| e.into())
    }

    fn flush(&mut self, ctx: &mut task::Context) -> Poll<(), Self::SinkError> {
        self.sink.flush(ctx).map_err(|e| e.into())
    }

    fn close(&mut self, ctx: &mut task::Context) -> Poll<(), Self::SinkError> {
        self.sink.close(ctx).map_err(|e| e.into())
    }
}

impl<S: Stream, E> Stream for SinkFromErr<S, E> {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self, ctx: &mut task::Context) -> Poll<Option<S::Item>, S::Error> {
        self.sink.poll(ctx)
    }
}
