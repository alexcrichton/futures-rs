use anchor_experiment::MovePinned;
use futures_core::{Future, FutureMove, Poll, Async};

/// Future for the `map` combinator, changing the type of a future.
///
/// This is created by the `Future::map` method.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Map<A, F> where A: Future {
    future: A,
    f: Option<F>,
}

pub fn new<A, F>(future: A, f: F) -> Map<A, F>
    where A: Future,
{
    Map {
        future: future,
        f: Some(f),
    }
}

impl<U, A, F> Future for Map<A, F>
    where A: Future,
          F: FnOnce(A::Item) -> U,
{
    type Item = U;
    type Error = A::Error;

    unsafe fn poll_unsafe(&mut self) -> Poll<U, A::Error> {
        let e = match self.future.poll_unsafe() {
            Ok(Async::Pending) => return Ok(Async::Pending),
            Ok(Async::Ready(e)) => Ok(e),
            Err(e) => Err(e),
        };
        e.map(self.f.take().expect("cannot poll Map twice"))
         .map(Async::Ready)
    }
}

impl<U, A, F> FutureMove for Map<A, F>
    where A: FutureMove,
          F: FnOnce(A::Item) -> U + MovePinned,
{
    fn poll_move(&mut self) -> Poll<U, A::Error> {
        let e = match self.future.poll_move() {
            Ok(Async::Pending) => return Ok(Async::Pending),
            Ok(Async::Ready(e)) => Ok(e),
            Err(e) => Err(e),
        };
        e.map(self.f.take().expect("cannot poll Map twice"))
         .map(Async::Ready)
    }
}
