use {StartSend, Sink, Stream, Poll, Async, AsyncSink};
use sync::BiLock;
use task::Task;

/// A `Stream` part of the split pair
pub struct SplitStream<S>(BiLock<S>);

impl<S: Stream> Stream for SplitStream<S> {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self, task: &Task) -> Poll<Option<S::Item>, S::Error> {
        match self.0.poll_lock(task) {
            Async::Ready(mut inner) => inner.poll(task),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

/// A `Sink` part of the split pair
pub struct SplitSink<S>(BiLock<S>);

impl<S: Sink> Sink for SplitSink<S> {
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, task: &Task, item: S::SinkItem)
        -> StartSend<S::SinkItem, S::SinkError>
    {
        match self.0.poll_lock(task) {
            Async::Ready(mut inner) => inner.start_send(task, item),
            Async::NotReady => Ok(AsyncSink::NotReady(item)),
        }
    }

    fn poll_complete(&mut self, task: &Task) -> Poll<(), S::SinkError> {
        match self.0.poll_lock(task) {
            Async::Ready(mut inner) => inner.poll_complete(task),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub fn split<S: Stream + Sink>(s: S) -> (SplitSink<S>, SplitStream<S>) {
    let (a, b) = BiLock::new(s);
    let read = SplitStream(a);
    let write = SplitSink(b);
    (write, read)
}
