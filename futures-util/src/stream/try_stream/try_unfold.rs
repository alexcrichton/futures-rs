use super::assert_stream;
use crate::fns::FnMut1;
use core::fmt;
use core::pin::Pin;
use futures_core::future::TryFuture;
use futures_core::ready;
use futures_core::stream::Stream;
use futures_core::task::{Context, Poll};
use pin_project_lite::pin_project;

/// Creates a `TryStream` from a seed and a closure returning a `TryFuture`.
///
/// This function is the dual for the `TryStream::try_fold()` adapter: while
/// `TryStream::try_fold()` reduces a `TryStream` to one single value,
/// `try_unfold()` creates a `TryStream` from a seed value.
///
/// `try_unfold()` will call the provided closure with the provided seed, then
/// wait for the returned `TryFuture` to complete with `(a, b)`. It will then
/// yield the value `a`, and use `b` as the next internal state.
///
/// If the closure returns `None` instead of `Some(TryFuture)`, then the
/// `try_unfold()` will stop producing items and return `Poll::Ready(None)` in
/// future calls to `poll()`.
///
/// In case of error generated by the returned `TryFuture`, the error will be
/// returned by the `TryStream`. The `TryStream` will then yield
/// `Poll::Ready(None)` in future calls to `poll()`.
///
/// This function can typically be used when wanting to go from the "world of
/// futures" to the "world of streams": the provided closure can build a
/// `TryFuture` using other library functions working on futures, and
/// `try_unfold()` will turn it into a `TryStream` by repeating the operation.
///
/// # Example
///
/// ```
/// # #[derive(Debug, PartialEq)]
/// # struct SomeError;
/// # futures::executor::block_on(async {
/// use futures::stream::{self, TryStreamExt};
///
/// let stream = stream::try_unfold(0, |state| async move {
///     if state < 0 {
///         return Err(SomeError);
///     }
///
///     if state <= 2 {
///         let next_state = state + 1;
///         let yielded = state * 2;
///         Ok(Some((yielded, next_state)))
///     } else {
///         Ok(None)
///     }
/// });
///
/// let result: Result<Vec<i32>, _> = stream.try_collect().await;
/// assert_eq!(result, Ok(vec![0, 2, 4]));
/// # });
/// ```
pub fn try_unfold<T, F, Fut, Item>(init: T, f: F) -> TryUnfold<T, F, Fut>
where
    F: FnMut(T) -> Fut,
    Fut: TryFuture<Ok = Option<(Item, T)>>,
{
    assert_stream::<Result<Item, Fut::Error>, _>(TryUnfold {
        f,
        state: Some(init),
        fut: None,
    })
}

/// See [`try_unfold`].
///
/// # Example
///
/// ```
/// # #[derive(Debug, PartialEq)]
/// # struct SomeError;
/// # futures::executor::block_on(async {
/// use futures_util::stream::{self, TryStreamExt};
///
/// let stream = stream::try_unfold_fns(0, |state: i32| async move {
///     if state < 0 {
///         return Err(SomeError);
///     }
///
///     if state <= 2 {
///         let next_state = state + 1;
///         let yielded = state * 2;
///         Ok(Some((yielded, next_state)))
///     } else {
///         Ok(None)
///     }
/// });
///
/// let result: Result<Vec<i32>, _> = stream.try_collect().await;
/// assert_eq!(result, Ok(vec![0, 2, 4]));
/// # });
/// ```
#[cfg(feature = "fntraits")]
#[cfg_attr(docsrs, doc(cfg(feature = "fntraits")))]
pub fn try_unfold_fns<T, F, Fut, Item>(init: T, f: F) -> TryUnfold<T, F, Fut>
where
    F: FnMut1<T, Output = Fut>,
    Fut: TryFuture<Ok = Option<(Item, T)>>,
{
    assert_stream::<Result<Item, Fut::Error>, _>(TryUnfold {
        f,
        state: Some(init),
        fut: None,
    })
}

pin_project! {
    /// Stream for the [`try_unfold`] function.
    #[must_use = "streams do nothing unless polled"]
    pub struct TryUnfold<T, F, Fut> {
        f: F,
        state: Option<T>,
        #[pin]
        fut: Option<Fut>,
    }
}

impl<T, F, Fut> fmt::Debug for TryUnfold<T, F, Fut>
where
    T: fmt::Debug,
    Fut: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TryUnfold")
            .field("state", &self.state)
            .field("fut", &self.fut)
            .finish()
    }
}

impl<T, F, Fut, Item> Stream for TryUnfold<T, F, Fut>
where
    F: FnMut1<T, Output = Fut>,
    Fut: TryFuture<Ok = Option<(Item, T)>>,
{
    type Item = Result<Item, Fut::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(state) = this.state.take() {
            this.fut.set(Some(this.f.call_mut(state)));
        }

        match this.fut.as_mut().as_pin_mut() {
            None => {
                // The future previously errored
                Poll::Ready(None)
            }
            Some(future) => {
                let step = ready!(future.try_poll(cx));
                this.fut.set(None);

                match step {
                    Ok(Some((item, next_state))) => {
                        *this.state = Some(next_state);
                        Poll::Ready(Some(Ok(item)))
                    }
                    Ok(None) => Poll::Ready(None),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
        }
    }
}
