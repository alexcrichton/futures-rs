use core::marker::Unpin;
use core::mem::PinMut;
use futures_core::stream::Stream;
use futures_core::task::{Context, Poll};

/// A stream combinator which will change the type of a stream from one
/// type to another.
///
/// This is produced by the `Stream::map` method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Map<S, F> {
    stream: S,
    f: F,
}

pub fn new<S, U, F>(s: S, f: F) -> Map<S, F>
    where S: Stream,
          F: FnMut(S::Item) -> U,
{
    Map {
        stream: s,
        f,
    }
}

impl<S, F> Map<S, F> {
    /// Acquires a reference to the underlying stream that this combinator is
    /// pulling from.
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Acquires a mutable reference to the underlying stream that this
    /// combinator is pulling from.
    ///
    /// Note that care must be taken to avoid tampering with the state of the
    /// stream which may otherwise confuse this combinator.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Consumes this combinator, returning the underlying stream.
    ///
    /// Note that this may discard intermediate state of this combinator, so
    /// care should be taken to avoid losing resources when this is called.
    pub fn into_inner(self) -> S {
        self.stream
    }

    unsafe_pinned!(stream -> S);
    unsafe_unpinned!(f -> F);
}

impl<S: Unpin, F> Unpin for Map<S, F> {}

/* TODO
// Forwarding impl of Sink from the underlying stream
impl<S, F> Sink for Map<S, F>
    where S: Sink
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    delegate_sink!(stream);
}
*/

impl<S, F, U> Stream for Map<S, F>
    where S: Stream,
          F: FnMut(S::Item) -> U,
{
    type Item = U;

    fn poll_next(mut self: PinMut<Self>, cx: &mut Context) -> Poll<Option<U>> {
        let option = ready!(self.stream().poll_next(cx));
        Poll::Ready(option.map(self.f()))
    }
}
