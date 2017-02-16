use Poll;
use stream::Stream;
use task::Task;

/// A stream combinator which will change the error type of a stream from one
/// type to another.
///
/// This is produced by the `Stream::map_err` method.
#[must_use = "streams do nothing unless polled"]
pub struct MapErr<S, F> {
    stream: S,
    f: F,
}

pub fn new<S, F, U>(s: S, f: F) -> MapErr<S, F>
    where S: Stream,
          F: FnMut(S::Error) -> U,
{
    MapErr {
        stream: s,
        f: f,
    }
}

// Forwarding impl of Sink from the underlying stream
impl<S, F> ::sink::Sink for MapErr<S, F>
    where S: ::sink::Sink
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

impl<S, F, U> Stream for MapErr<S, F>
    where S: Stream,
          F: FnMut(S::Error) -> U,
{
    type Item = S::Item;
    type Error = U;

    fn poll(&mut self, task: &Task) -> Poll<Option<S::Item>, U> {
        self.stream.poll(task).map_err(&mut self.f)
    }
}
