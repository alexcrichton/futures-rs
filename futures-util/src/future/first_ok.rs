use crate::future::{Fuse, FutureExt};
use alloc::vec::Vec;
use core::iter::FromIterator;
use core::pin::Pin;
use futures_core::future::{FusedFuture, Future, TryFuture};
use futures_core::task::{Context, Poll};

/// Future for the [`first_ok()`] function.
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug, Clone)]
pub struct FirstOk<F> {
    // Critical safety invariant: after FirstAll is created, this vector can
    // never be reallocated, nor can its contents be moved, in order to ensure
    // that Pin is upheld.
    futures: Vec<F>,
}

// Safety: once created, the contents of the vector don't change, and they'll
// remain in place permanently.
impl<F> Unpin for FirstOk<F> {}

impl<T, E, F> Future for FirstOk<F>
where
    F: Future<Output = Result<T, E>> + FusedFuture,
{
    type Output = Result<T, E>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        /// Helper enum to track our state as we poll each future
        enum State<E> {
            /// Haven't seen any errors
            NoErrors,

            /// The last error we've seen
            SeenError(E),

            /// At least 1 future is still pending; there's no need to
            /// track errors
            SeenPending,
        }

        use State::*;

        impl<E> State<E> {
            fn apply_error(&mut self, err: E) {
                match self {
                    SeenError(..) | NoErrors => *self = SeenError(err),
                    SeenPending => {}
                }
            }

            fn apply_pending(&mut self) {
                *self = SeenPending;
            }
        }

        let mut state = State::NoErrors;

        for fut in self.get_mut().futures.iter_mut() {
            if !fut.is_terminated() {
                // Safety: we promise that the future is never moved out of the vec,
                // and that the vec never reallocates once FirstOk has been created
                // (specifically after the first poll)
                let pinned = unsafe { Pin::new_unchecked(fut) };
                match pinned.try_poll(cx) {
                    Poll::Ready(Ok(out)) => return Poll::Ready(Ok(out)),
                    Poll::Ready(Err(err)) => state.apply_error(err),
                    Poll::Pending => state.apply_pending(),
                }
            }
        }

        match state {
            SeenError(err) => Poll::Ready(Err(err)),
            SeenPending | NoErrors => Poll::Pending,
        }
    }
}

// We don't provide FusedFuture, because the overhead of implementing it (
// which requires clearing the vector after Ready is returned) is precisely
// the same as using .fuse()

impl<T, E, F> FromIterator<F> for FirstOk<F>
where
    F: FusedFuture + Future<Output = Result<T, E>>,
{
    fn from_iter<I: IntoIterator<Item = F>>(iter: I) -> Self {
        first_ok_fused(iter)
    }
}

/// Creates a new future which will return the result of the first successful
/// future in a list of futures.
///
/// The returned future will wait for any future within `iter` to be ready
/// and `Ok`. Unlike `first_all`, this will only return the first successful
/// completion, or the last error if none complete with `Ok`. This is useful
/// in contexts where any success is desired and failures are ignored, unless
/// all the futures fail.
///
/// `first_ok_fused` requires [`FusedFuture`], in order to track which futures have
/// completed with errors and which are still pending. Many futures already
/// implement this trait. Use [`first_ok`] if you have futures which do not
/// implement [`FusedFuture`].
///
/// Any futures in the list that have already terminated will be ignored.
///
/// This function is only available when the `std` or `alloc` feature of this
/// library is activated, and it is activated by default.
///
/// # Panics
///
/// This function will panic if the iterator specified contains no unterminated
/// items.
pub fn first_ok_fused<T, E, I>(futures: I) -> FirstOk<I::Item>
where
    I: IntoIterator,
    I::Item: FusedFuture + Future<Output = Result<T, E>>,
{
    let futures = Vec::from_iter(futures);

    assert!(
        !futures.is_empty(),
        "Need at least 1 non-terminated future for first_ok"
    );

    FirstOk { futures }
}

/// Creates a new future which will return the result of the first successful
/// future in a list of futures.
///
/// The returned future will wait for any future within `iter` to be ready
/// and `Ok`. Unlike `first_all`, this will only return the first successful
/// completion, or the last error if none complete with `Ok`. This is useful
/// in contexts where any success is desired and failures are ignored, unless
/// all the futures fail.
///
/// If your future implements [`FusedFuture`], prefer [`first_ok_fused`],
/// which will have less overhead.
///
/// This function is only available when the `std` or `alloc` feature of this
/// library is activated, and it is activated by default.
///
/// # Panics
///
/// This function will panic if the iterator specified contains no unterminated
/// items.
pub fn first_ok<T, E, I>(futures: I) -> FirstOk<Fuse<I::Item>>
where
    I: IntoIterator,
    I::Item: Future<Output = Result<T, E>>,
{
    first_ok_fused(futures.into_iter().map(|fut| fut.fuse()))
}

#[test]
fn test_first_ok_ok() {
    use crate::task::noop_waker_ref;
    use futures_channel::oneshot::channel;

    let mut futures = vec![];
    let mut senders = vec![];

    for _ in 0..10 {
        let (send, recv) = channel();
        futures.push(recv);
        senders.push(send);
    }

    let (send, recv) = channel();
    futures.push(recv);

    for _ in 0..10 {
        let (send, recv) = channel();
        futures.push(recv);
        senders.push(send);
    }

    let mut fut = first_ok(futures);
    let mut context = Context::from_waker(noop_waker_ref());

    let poll = fut.poll_unpin(&mut context);
    assert_eq!(poll, Poll::Pending);

    send.send(10).unwrap();
    let poll = fut.poll_unpin(&mut context);
    assert_eq!(poll, Poll::Ready(Ok(10)));
}

#[test]
fn test_first_ok_err() {
    use crate::task::noop_waker_ref;
    use futures_channel::oneshot::{channel, Canceled};

    let mut futures = vec![];
    let mut senders = vec![];

    for _ in 0..10 {
        let (send, recv) = channel::<u32>();
        futures.push(recv);
        senders.push(send);
    }

    let mut fut = first_ok(futures);
    let mut context = Context::from_waker(noop_waker_ref());

    // Dropping a sender causes an error in the receiver.
    for sender in senders.into_iter() {
        let poll = fut.poll_unpin(&mut context);
        assert_eq!(poll, Poll::Pending);

        drop(sender);
    }

    let poll = fut.poll_unpin(&mut context);
    assert_eq!(poll, Poll::Ready(Err(Canceled)));
}
