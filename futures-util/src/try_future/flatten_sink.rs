use core::marker::Unpin;
use core::mem::PinMut;

use futures_core::{task, Poll, TryFuture};
use futures_sink::Sink;

#[derive(Debug)]
enum State<F, S> {
    Waiting(F),
    Ready(S),
    Closed,
}
use self::State::*;

/// Future for the `flatten_sink` combinator, flattening a
/// future-of-a-sink to get just the result of the final sink as a sink.
///
/// This is created by the `Future::flatten_sink` method.
#[derive(Debug)]
pub struct FlattenSink<F, S>(State<F, S>);

impl<F: Unpin, S: Unpin> Unpin for FlattenSink<F, S> {}

impl<F, S> FlattenSink<F, S> {
    fn project_pin<'a>(self: PinMut<'a, Self>)
        -> State<PinMut<'a, F>, PinMut<'a, S>>
    {
        unsafe {
            match &mut PinMut::get_mut(self).0 {
                Waiting(f) => Waiting(PinMut::new_unchecked(f)),
                Ready(s) => Ready(PinMut::new_unchecked(s)),
                Closed => Closed,
            }
        }
    }
}

impl<F, S> Sink for FlattenSink<F, S>
where
    F: TryFuture<Item = S>,
    S: Sink<SinkError = F::Error>,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn poll_ready(mut self: PinMut<Self>, cx: &mut task::Context)
        -> Poll<Result<(), Self::SinkError>>
    {
        let resolved_stream = match self.reborrow().project_pin() {
            Ready(s) => return s.poll_ready(cx),
            Waiting(f) => try_ready!(f.try_poll(cx)),
            Closed => panic!("poll_ready called after eof"),
        };
        PinMut::set(self.reborrow(), FlattenSink(Ready(resolved_stream)));
        if let Ready(resolved_stream) = self.project_pin() {
            resolved_stream.poll_ready(cx)
        } else {
            unreachable!()
        }
    }

    fn start_send(self: PinMut<Self>, item: Self::SinkItem)
        -> Result<(), Self::SinkError>
    {
        match self.project_pin() {
            Ready(s) => s.start_send(item),
            Waiting(_) => panic!("poll_ready not called first"),
            Closed => panic!("start_send called after eof"),
        }
    }

    fn poll_flush(self: PinMut<Self>, cx: &mut task::Context)
        -> Poll<Result<(), Self::SinkError>>
    {
        match self.project_pin() {
            Ready(s) => s.poll_flush(cx),
            // if sink not yet resolved, nothing written ==> everything flushed
            Waiting(_) => Poll::Ready(Ok(())),
            Closed => panic!("poll_flush called after eof"),
        }
    }

    fn poll_close(mut self: PinMut<Self>, cx: &mut task::Context)
        -> Poll<Result<(), Self::SinkError>>
    {
        let res = match self.reborrow().project_pin() {
            Ready(s) => s.poll_close(cx),
            Waiting(_) | Closed => Poll::Ready(Ok(())),
        };
        if res.is_ready() {
            PinMut::set(self, FlattenSink(Closed));
        }
        res
    }
}

pub fn new<F, S>(fut: F) -> FlattenSink<F, S>
where
    F: TryFuture<Item = S>,
    S: Sink<SinkError = F::Error>,
{
    FlattenSink(Waiting(fut))
}
