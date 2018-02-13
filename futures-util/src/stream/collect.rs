use std::prelude::v1::*;

use std::mem;

use anchor_experiment::MovePinned;
use futures_core::{Future, FutureMove, Poll, Async, Stream};

/// A future which collects all of the values of a stream into a vector.
///
/// This future is created by the `Stream::collect` method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Collect<S> where S: Stream {
    stream: S,
    items: Vec<S::Item>,
}

pub fn new<S>(s: S) -> Collect<S>
    where S: Stream,
{
    Collect {
        stream: s,
        items: Vec::new(),
    }
}

impl<S: Stream> Collect<S> {
    fn finish(&mut self) -> Vec<S::Item> {
        mem::replace(&mut self.items, Vec::new())
    }
}

impl<S> Future for Collect<S>
    where S: Stream,
{
    type Item = Vec<S::Item>;
    type Error = S::Error;

    poll_safe!();
}

impl<S> FutureMove for Collect<S>
    where S: Stream + MovePinned,
{
    fn poll_move(&mut self) -> Poll<Vec<S::Item>, S::Error> {
        loop {
            match self.stream.poll() {
                Ok(Async::Ready(Some(e))) => self.items.push(e),
                Ok(Async::Ready(None)) => return Ok(Async::Ready(self.finish())),
                Ok(Async::Pending) => return Ok(Async::Pending),
                Err(e) => {
                    self.finish();
                    return Err(e)
                }
            }
        }
    }
}
