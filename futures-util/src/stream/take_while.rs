use futures_core::{Async, Poll, IntoFuture, Future, Stream};
use futures_core::task;
use futures_sink::{StartSend, Sink};

/// A stream combinator which takes elements from a stream while a predicate
/// holds.
///
/// This structure is produced by the `Stream::take_while` method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct TakeWhile<S, P, R> where S: Stream, R: IntoFuture {
    stream: S,
    pred: P,
    pending: Option<(R::Future, S::Item)>,
    done_taking: bool,
}

pub fn new<S, P, R>(s: S, p: P) -> TakeWhile<S, P, R>
    where S: Stream,
          P: FnMut(&S::Item) -> R,
          R: IntoFuture<Item=bool, Error=S::Error>,
{
    TakeWhile {
        stream: s,
        pred: p,
        pending: None,
        done_taking: false,
    }
}

impl<S, P, R> TakeWhile<S, P, R> where S: Stream, R: IntoFuture {
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
}

// Forwarding impl of Sink from the underlying stream
impl<S, P, R> Sink for TakeWhile<S, P, R>
    where S: Sink + Stream, R: IntoFuture
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, ctx: &mut task::Context, item: S::SinkItem) -> StartSend<S::SinkItem, S::SinkError> {
        self.stream.start_send(ctx, item)
    }

    fn flush(&mut self, ctx: &mut task::Context) -> Poll<(), S::SinkError> {
        self.stream.flush(ctx)
    }

    fn close(&mut self, ctx: &mut task::Context) -> Poll<(), S::SinkError> {
        self.stream.close(ctx)
    }
}

impl<S, P, R> Stream for TakeWhile<S, P, R>
    where S: Stream,
          P: FnMut(&S::Item) -> R,
          R: IntoFuture<Item=bool, Error=S::Error>,
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self, ctx: &mut task::Context) -> Poll<Option<S::Item>, S::Error> {
        if self.done_taking {
            return Ok(Async::Ready(None));
        }

        if self.pending.is_none() {
            let item = match try_ready!(self.stream.poll(ctx)) {
                Some(e) => e,
                None => return Ok(Async::Ready(None)),
            };
            self.pending = Some(((self.pred)(&item).into_future(), item));
        }

        assert!(self.pending.is_some());
        match self.pending.as_mut().unwrap().0.poll(ctx) {
            Ok(Async::Ready(true)) => {
                let (_, item) = self.pending.take().unwrap();
                Ok(Async::Ready(Some(item)))
            },
            Ok(Async::Ready(false)) => {
                self.done_taking = true;
                Ok(Async::Ready(None))
            }
            Ok(Async::Pending) => Ok(Async::Pending),
            Err(e) => {
                self.pending = None;
                Err(e)
            }
        }
    }
}
