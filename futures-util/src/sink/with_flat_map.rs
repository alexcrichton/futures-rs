use core::marker::PhantomData;

use futures_core::{Poll, Async, Stream};
use futures_core::task;
use futures_sink::{Sink, StartSend, AsyncSink};

/// Sink for the `Sink::with_flat_map` combinator, chaining a computation that returns an iterator
/// to run prior to pushing a value into the underlying sink
#[derive(Debug)]
#[must_use = "sinks do nothing unless polled"]
pub struct WithFlatMap<S, U, F, St>
where
    S: Sink,
    F: FnMut(U) -> St,
    St: Stream<Item = S::SinkItem, Error=S::SinkError>,
{
    sink: S,
    f: F,
    stream: Option<St>,
    buffer: Option<S::SinkItem>,
    _phantom: PhantomData<fn(U)>,
}

pub fn new<S, U, F, St>(sink: S, f: F) -> WithFlatMap<S, U, F, St>
where
    S: Sink,
    F: FnMut(U) -> St,
    St: Stream<Item = S::SinkItem, Error=S::SinkError>,
{
    WithFlatMap {
        sink: sink,
        f: f,
        stream: None,
        buffer: None,
        _phantom: PhantomData,
    }
}

impl<S, U, F, St> WithFlatMap<S, U, F, St>
where
    S: Sink,
    F: FnMut(U) -> St,
    St: Stream<Item = S::SinkItem, Error=S::SinkError>,
{
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

    fn try_empty_stream(&mut self, ctx: &mut task::Context) -> Poll<(), S::SinkError> {
        if let Some(x) = self.buffer.take() {
            if let AsyncSink::Pending(x) = self.sink.start_send(ctx, x)? {
                self.buffer = Some(x);
                return Ok(Async::Pending);
            }
        }
        if let Some(mut stream) = self.stream.take() {
            while let Some(x) = try_ready!(stream.poll(ctx)) {
                if let AsyncSink::Pending(x) = self.sink.start_send(ctx, x)? {
                    self.stream = Some(stream);
                    self.buffer = Some(x);
                    return Ok(Async::Pending);
                }
            }
        }
        Ok(Async::Ready(()))
    }
}

impl<S, U, F, St> Stream for WithFlatMap<S, U, F, St>
where
    S: Stream + Sink,
    F: FnMut(U) -> St,
    St: Stream<Item = S::SinkItem, Error=S::SinkError>,
{
    type Item = S::Item;
    type Error = S::Error;
    fn poll(&mut self, ctx: &mut task::Context) -> Poll<Option<S::Item>, S::Error> {
        self.sink.poll(ctx)
    }
}

impl<S, U, F, St> Sink for WithFlatMap<S, U, F, St>
where
    S: Sink,
    F: FnMut(U) -> St,
    St: Stream<Item = S::SinkItem, Error=S::SinkError>,
{
    type SinkItem = U;
    type SinkError = S::SinkError;
    fn start_send(&mut self, ctx: &mut task::Context, i: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        if self.try_empty_stream(ctx)?.is_not_ready() {
            return Ok(AsyncSink::Pending(i));
        }
        assert!(self.stream.is_none());
        self.stream = Some((self.f)(i));
        self.try_empty_stream(ctx)?;
        Ok(AsyncSink::Ready)
    }
    fn flush(&mut self, ctx: &mut task::Context) -> Poll<(), Self::SinkError> {
        if self.try_empty_stream(ctx)?.is_not_ready() {
            return Ok(Async::Pending);
        }
        self.sink.flush(ctx)
    }
    fn close(&mut self, ctx: &mut task::Context) -> Poll<(), Self::SinkError> {
        if self.try_empty_stream(ctx)?.is_not_ready() {
            return Ok(Async::Pending);
        }
        assert!(self.stream.is_none());
        self.sink.close(ctx)
    }
}
