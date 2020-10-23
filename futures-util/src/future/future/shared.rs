use crate::notifier::{Notifier, NULL_WAKER_KEY};
use crate::task::waker_ref;
use futures_core::future::{FusedFuture, Future};
use futures_core::task::{Context, Poll};
use std::cell::UnsafeCell;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

/// Future for the [`shared`](super::FutureExt::shared) method.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Shared<Fut: Future> {
    inner: Option<Arc<Inner<Fut>>>,
    waker_key: usize,
}

struct Inner<Fut: Future> {
    future_or_output: UnsafeCell<FutureOrOutput<Fut>>,
    notifier: Arc<Notifier>,
}

// The future itself is polled behind the `Arc`, so it won't be moved
// when `Shared` is moved.
impl<Fut: Future> Unpin for Shared<Fut> {}

impl<Fut: Future> fmt::Debug for Shared<Fut> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("inner", &self.inner)
            .field("waker_key", &self.waker_key)
            .finish()
    }
}

impl<Fut: Future> fmt::Debug for Inner<Fut> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner").finish()
    }
}

enum FutureOrOutput<Fut: Future> {
    Future(Fut),
    Output(Fut::Output),
}

unsafe impl<Fut> Send for Inner<Fut>
where
    Fut: Future + Send,
    Fut::Output: Send + Sync,
{
}

unsafe impl<Fut> Sync for Inner<Fut>
where
    Fut: Future + Send,
    Fut::Output: Send + Sync,
{
}

impl<Fut: Future> Shared<Fut> {
    pub(super) fn new(future: Fut) -> Shared<Fut> {
        let inner = Inner {
            future_or_output: UnsafeCell::new(FutureOrOutput::Future(future)),
            notifier: Arc::new(Notifier::new()),
        };

        Shared {
            inner: Some(Arc::new(inner)),
            waker_key: NULL_WAKER_KEY,
        }
    }
}

impl<Fut> Shared<Fut>
where
    Fut: Future,
    Fut::Output: Clone,
{
    /// Returns [`Some`] containing a reference to this [`Shared`]'s output if
    /// it has already been computed by a clone or [`None`] if it hasn't been
    /// computed yet or this [`Shared`] already returned its output from
    /// [`poll`](Future::poll).
    pub fn peek(&self) -> Option<&Fut::Output> {
        if let Some(inner) = self.inner.as_ref() {
            if inner.notifier.is_complete() {
                unsafe {
                    return Some(inner.output());
                }
            }
        }
        None
    }
}

impl<Fut> Inner<Fut>
where
    Fut: Future,
    Fut::Output: Clone,
{
    /// Safety: callers must first ensure that `self.notifier.is_complete()`
    /// is `true`
    unsafe fn output(&self) -> &Fut::Output {
        match &*self.future_or_output.get() {
            FutureOrOutput::Output(ref item) => &item,
            FutureOrOutput::Future(_) => unreachable!(),
        }
    }
    /// Registers the current task to receive a wakeup when we are awoken.
    fn record_waker(&self, waker_key: &mut usize, cx: &mut Context<'_>) {
        self.notifier.record_waker(waker_key, cx);
    }

    /// Safety: callers must first ensure that `self.notifier.is_complete()`
    /// is `true`
    unsafe fn take_or_clone_output(self: Arc<Self>) -> Fut::Output {
        match Arc::try_unwrap(self) {
            Ok(inner) => match inner.future_or_output.into_inner() {
                FutureOrOutput::Output(item) => item,
                FutureOrOutput::Future(_) => unreachable!(),
            },
            Err(inner) => inner.output().clone(),
        }
    }
}

impl<Fut> FusedFuture for Shared<Fut>
where
    Fut: Future,
    Fut::Output: Clone,
{
    fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }
}

impl<Fut> Future for Shared<Fut>
where
    Fut: Future,
    Fut::Output: Clone,
{
    type Output = Fut::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use crate::notifier::State::*;
        let this = &mut *self;

        let inner = this
            .inner
            .take()
            .expect("Shared future polled again after completion");

        // Fast path for when the wrapped future has already completed
        if inner.notifier.is_complete() {
            // Safety: We're in the COMPLETE state
            return unsafe { Poll::Ready(inner.take_or_clone_output()) };
        }

        inner.record_waker(&mut this.waker_key, cx);

        match inner.notifier.try_start_poll() {
            Idle => {
                // Lock acquired, fall through
            }
            Polling => {
                // Another task is currently polling, at this point we just want
                // to ensure that the waker for this task is registered
                this.inner = Some(inner);
                return Poll::Pending;
            }
            Complete => {
                // Safety: We're in the COMPLETE state
                return unsafe { Poll::Ready(inner.take_or_clone_output()) };
            }
        }

        let waker = waker_ref(&inner.notifier);
        let mut cx = Context::from_waker(&waker);

        let _reset = inner.notifier.reset_guard();

        let output = {
            let future = unsafe {
                match &mut *inner.future_or_output.get() {
                    FutureOrOutput::Future(fut) => Pin::new_unchecked(fut),
                    _ => unreachable!(),
                }
            };

            match future.poll(&mut cx) {
                Poll::Pending => {
                    inner.notifier.set_idle();
                    drop(_reset);
                    this.inner = Some(inner);
                    return Poll::Pending;
                }
                Poll::Ready(output) => output,
            }
        };

        unsafe {
            *inner.future_or_output.get() = FutureOrOutput::Output(output);
        }

        // Complete the future
        inner.notifier.finish_poll(_reset);

        // Safety: We're in the COMPLETE state
        unsafe { Poll::Ready(inner.take_or_clone_output()) }
    }
}

impl<Fut> Clone for Shared<Fut>
where
    Fut: Future,
{
    fn clone(&self) -> Self {
        Shared {
            inner: self.inner.clone(),
            waker_key: NULL_WAKER_KEY,
        }
    }
}

impl<Fut> Drop for Shared<Fut>
where
    Fut: Future,
{
    fn drop(&mut self) {
        if let Some(ref inner) = self.inner {
            inner.notifier.remove(self.waker_key);
        }
    }
}
