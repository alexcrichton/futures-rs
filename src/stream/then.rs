use {Async, IntoFuture, Future, Poll};
use stream::Stream;
use task::Task;

/// A stream combinator which chains a computation onto each item produced by a
/// stream.
///
/// This structure is produced by the `Stream::then` method.
#[must_use = "streams do nothing unless polled"]
pub struct Then<S, F, U>
    where U: IntoFuture,
{
    stream: S,
    future: Option<U::Future>,
    f: F,
}

pub fn new<S, F, U>(s: S, f: F) -> Then<S, F, U>
    where S: Stream,
          F: FnMut(Result<S::Item, S::Error>) -> U,
          U: IntoFuture,
{
    Then {
        stream: s,
        future: None,
        f: f,
    }
}

// Forwarding impl of Sink from the underlying stream
impl<S, F, U> ::sink::Sink for Then<S, F, U>
    where S: ::sink::Sink, U: IntoFuture,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, task: &Task, item: S::SinkItem) -> ::StartSend<S::SinkItem, S::SinkError> {
        self.stream.start_send(task, item)
    }

    fn poll_complete(&mut self, task: &Task) -> Poll<(), S::SinkError> {
        self.stream.poll_complete(task)
    }
}

impl<S, F, U> Stream for Then<S, F, U>
    where S: Stream,
          F: FnMut(Result<S::Item, S::Error>) -> U,
          U: IntoFuture,
{
    type Item = U::Item;
    type Error = U::Error;

    fn poll(&mut self, task: &Task) -> Poll<Option<U::Item>, U::Error> {
        if self.future.is_none() {
            let item = match self.stream.poll(task) {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => return Ok(Async::Ready(None)),
                Ok(Async::Ready(Some(e))) => Ok(e),
                Err(e) => Err(e),
            };
            self.future = Some((self.f)(item).into_future());
        }
        assert!(self.future.is_some());
        match self.future.as_mut().unwrap().poll(task) {
            Ok(Async::Ready(e)) => {
                self.future = None;
                Ok(Async::Ready(Some(e)))
            }
            Err(e) => {
                self.future = None;
                Err(e)
            }
            Ok(Async::NotReady) => Ok(Async::NotReady)
        }
    }
}
