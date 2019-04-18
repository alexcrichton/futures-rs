use core::pin::Pin;
use futures_core::future::Future;
use futures_core::stream::{FusedStream, Stream};
use futures_core::task::{Context, Poll};
use pin_utils::{unsafe_pinned, unsafe_unpinned};

/// Creates a `Stream` from a seed and a closure returning a `Future`.
///
/// This function is the dual for the `Stream::fold()` adapter: while
/// `Stream::fold()` reduces a `Stream` to one single value, `unfold()` creates a
/// `Stream` from a seed value.
///
/// `unfold()` will call the provided closure with the provided seed, then wait
/// for the returned `Future` to complete with `(a, b)`. It will then yield the
/// value `a`, and use `b` as the next internal state.
///
/// If the closure returns `None` instead of `Some(Future)`, then the `unfold()`
/// will stop producing items and return `Poll::Ready(None)` in future
/// calls to `poll()`.
///
/// In case of error generated by the returned `Future`, the error will be
/// returned by the `Stream`.  The `Stream` will then yield
/// `Poll::Ready(None)` in future calls to `poll()`.
///
/// This function can typically be used when wanting to go from the "world of
/// futures" to the "world of streams": the provided closure can build a
/// `Future` using other library functions working on futures, and `unfold()`
/// will turn it into a `Stream` by repeating the operation.
///
/// # Example
///
/// ```
/// use futures::executor::block_on;
/// use futures::future;
/// use futures::stream::{self, StreamExt};
///
/// let mut stream = stream::unfold(0, |state| {
///     if state <= 2 {
///         let next_state = state + 1;
///         let yielded = state  * 2;
///         future::ready(Some((yielded, next_state)))
///     } else {
///         future::ready(None)
///     }
/// });
///
/// let result = block_on(stream.collect::<Vec<i32>>());
/// assert_eq!(result, vec![0, 2, 4]);
/// ```
pub fn unfold<T, F, Fut, It>(init: T, f: F) -> Unfold<T, F, Fut>
    where F: FnMut(T) -> Fut,
          Fut: Future<Output = Option<(It, T)>>,
{
    Unfold {
        f,
        state: Some(init),
        fut: None,
    }
}

/// Stream for the [`unfold`] function.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Unfold<T, F, Fut> {
    f: F,
    state: Option<T>,
    fut: Option<Fut>,
}

impl<T, F, Fut: Unpin> Unpin for Unfold<T, F, Fut> {}

impl<T, F, Fut> Unfold<T, F, Fut> {
    unsafe_unpinned!(f: F);
    unsafe_unpinned!(state: Option<T>);
    unsafe_pinned!(fut: Option<Fut>);
}

impl<T, F, Fut> FusedStream for Unfold<T, F, Fut> {
    fn is_terminated(&self) -> bool {
        self.state.is_none() && self.fut.is_none()
    }
}

impl<T, F, Fut, It> Stream for Unfold<T, F, Fut>
    where F: FnMut(T) -> Fut,
          Fut: Future<Output = Option<(It, T)>>,
{
    type Item = It;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<It>> {
        if let Some(state) = self.as_mut().state().take() {
            let fut = (self.as_mut().f())(state);
            self.as_mut().fut().set(Some(fut));
        }

        let step = ready!(self.as_mut().fut().as_pin_mut().unwrap().poll(cx));
        self.as_mut().fut().set(None);

        if let Some((item, next_state)) = step {
            *self.as_mut().state() = Some(next_state);
            return Poll::Ready(Some(item))
        } else {
            return Poll::Ready(None)
        }
    }
}
