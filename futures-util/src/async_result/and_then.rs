use core::mem::Pin;

use futures_core::{Async, Poll};
use futures_core::task;

use futures_core::AsyncResult;

/// Async for the `and_then` combinator, chaining a computation onto the end of
/// another future which completes successfully.
///
/// This is created by the `Async::and_then` method.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct AndThen<A, B, F> {
    state: State<A, B, F>,
}

#[derive(Debug)]
enum State<Fut1, Fut2, F> {
    First(Fut1, Option<F>),
    Second(Fut2),
}

pub fn new<A, B, F>(future: A, f: F) -> AndThen<A, B, F> {
    AndThen {
        state: State::First(future, Some(f)),
    }
}

impl<A, B, F> Async for AndThen<A, B, F>
    where A: AsyncResult,
          B: AsyncResult<Error = A::Error>,
          F: FnOnce(A::Item) -> B,
{
    type Output = Result<B::Item, B::Error>;

    fn poll(mut self: Pin<Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        loop {
            // safe to `get_mut` here because we don't move out
            let fut2 = match unsafe { Pin::get_mut(&mut self) }.state {
                State::First(ref mut fut1, ref mut data) => {
                    // safe to create a new `Pin` because `fut1` will never move
                    // before it's dropped.
                    match unsafe { Pin::new_unchecked(fut1) }.poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Ready(Ok(v)) => {
                            (data.take().unwrap())(v)
                        }
                    }
                }
                State::Second(ref mut fut2) => {
                    // safe to create a new `Pin` because `fut2` will never move
                    // before it's dropped; once we're in `Chain::Second` we stay
                    // there forever.
                    return unsafe { Pin::new_unchecked(fut2) }.poll(cx)
                }
            };

            // safe because we're using the `&mut` to do an assignment, not for moving out
            unsafe {
                // note: it's safe to move the `fut2` here because we haven't yet polled it
                Pin::get_mut(&mut self).state = State::Second(fut2);
            }
        }
    }
}
