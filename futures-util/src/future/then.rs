use super::Chain;
use core::mem::PinMut;
use futures_core::future::Future;
use futures_core::task::{Context, Poll};

/// Future for the `then` combinator, chaining computations on the end of
/// another future regardless of its outcome.
///
/// This is created by the `Future::then` method.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Then<Fut1, Fut2, F> {
    chain: Chain<Fut1, Fut2, F>,
}

impl<Fut1, Fut2, F> Then<Fut1, Fut2, F>
    where Fut1: Future,
          Fut2: Future,
{
    unsafe_pinned!(chain -> Chain<Fut1, Fut2, F>);

    /// Creates a new `Then`.
    pub(super) fn new(future: Fut1, async_op: F) -> Then<Fut1, Fut2, F> {
        Then {
            chain: Chain::new(future, async_op),
        }
    }
}

impl<Fut1, Fut2, F> Future for Then<Fut1, Fut2, F>
    where Fut1: Future,
          Fut2: Future,
          F: FnOnce(Fut1::Output) -> Fut2,
{
    type Output = Fut2::Output;

    fn poll(mut self: PinMut<Self>, cx: &mut Context) -> Poll<Fut2::Output> {
        self.chain().poll(cx, |output, async_op| async_op(output))
    }
}
