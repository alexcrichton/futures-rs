use futures_core::{Async, Future, IntoFuture, Poll, Stream};
use futures_core::task;
use futures_sink::{Sink, StartSend};

/// A stream combinator used to filter the results of a stream and only yield
/// some values.
///
/// This structure is produced by the `Stream::filter` method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Filter<S, P, R>
    where S: Stream,
          P: FnMut(&S::Item) -> R,
          R: IntoFuture<Item=bool, Error=S::Error>,
{
    stream: S,
    pred: P,
    pending: Option<(R::Future, S::Item)>,
}

pub fn new<S, P, R>(s: S, pred: P) -> Filter<S, P, R>
    where S: Stream,
          P: FnMut(&S::Item) -> R,
          R: IntoFuture<Item=bool, Error=S::Error>,
{
    Filter {
        stream: s,
        pred: pred,
        pending: None,
    }
}

impl<S, P, R> Filter<S, P, R>
    where S: Stream,
          P: FnMut(&S::Item) -> R,
          R: IntoFuture<Item=bool, Error=S::Error>,
{
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
impl<S, P, R> Sink for Filter<S, P, R>
    where S: Stream,
          P: FnMut(&S::Item) -> R,
          R: IntoFuture<Item=bool, Error=S::Error>,
          S: Sink,
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

impl<S, P, R> Stream for Filter<S, P, R>
    where S: Stream,
          P: FnMut(&S::Item) -> R,
          R: IntoFuture<Item=bool, Error=S::Error>,
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self, ctx: &mut task::Context) -> Poll<Option<S::Item>, S::Error> {
        loop {
            if self.pending.is_none() {
                let item = match try_ready!(self.stream.poll(ctx)) {
                    Some(e) => e,
                    None => return Ok(Async::Ready(None)),
                };
                let fut = ((self.pred)(&item)).into_future();
                self.pending = Some((fut, item));
            }

            match self.pending.as_mut().unwrap().0.poll(ctx) {
                Ok(Async::Ready(true)) => {
                    let (_, item) = self.pending.take().unwrap();
                    return Ok(Async::Ready(Some(item)));
                }
                Ok(Async::Ready(false)) => self.pending = None,
                Ok(Async::Pending) => return Ok(Async::Pending),
                Err(e) => {
                    self.pending = None;
                    return Err(e)
                }
            }
        }
    }
}
