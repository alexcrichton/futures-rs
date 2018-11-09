use crate::notifier::{Notifier, NULL_WAKER_KEY};
use crate::task::{waker_ref, ArcWake};
use alloc::fmt;
use alloc::sync::Arc;
use core::cell::UnsafeCell;
use core::marker::Unpin;
use core::pin::Pin;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering::SeqCst;
use futures_core::stream::Stream;
use futures_core::task::{Context, Poll};

/// A stream that is cloneable and can be polled in multiple threads.
/// Use the [`shared`](crate::StreamExt::shared) combinator method to convert
/// any stream into a `Shared` sream.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Shared<S: Stream> {
    idx: usize,
    waker_key: usize,
    inner: Arc<Inner<S>>,
}

struct Seat<T> {
    refcount: AtomicUsize,
    state: AtomicUsize,
    val: UnsafeCell<Option<T>>,
}

impl<T> Default for Seat<T> {
    fn default() -> Self {
        Seat {
            refcount: AtomicUsize::new(0),
            state: AtomicUsize::new(0),
            val: UnsafeCell::new(None),
        }
    }
}

struct Inner<S: Stream> {
    buffer: Box<[Seat<S::Item>]>,
    stream: UnsafeCell<S>, // Should this be an Option, so we can drop it if we hit the end?
    notifier: Arc<Notifier>,
}

unsafe impl<S> Send for Inner<S>
where
    S: Stream + Send,
    S::Item: Send + Sync,
{
}

unsafe impl<S> Sync for Inner<S>
where
    S: Stream + Send,
    S::Item: Send + Sync,
{
}

// The stream itself is polled behind the `Arc`, so it won't be moved
// when `Shared` is moved.
impl<S: Stream> Unpin for Shared<S> {}

impl<S: Stream> fmt::Debug for Inner<S> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Inner")
            .field("capacity", &(self.buffer.len() - 1))
            .finish()
    }
}

unsafe impl<T> Send for Seat<T> where T: Send {}
unsafe impl<T> Sync for Seat<T> where T: Send + Sync {}

// State for a Seat
/// The seat is empty
const EMPTY: usize = 0;
/// The seat is full/has a completed value
const FULL: usize = 1;
/// The queue is full, and we are waiting for the another
/// seat to become available.
const WAITING: usize = 3;

impl<S: Stream> Shared<S> {
    pub(super) fn new(stream: S, cap: usize) -> Shared<S> {
        let mut buffer = Vec::with_capacity(cap + 1);
        // The first seat will initially have one reference to it.
        buffer.push(Seat {
            refcount: AtomicUsize::new(1),
            state: AtomicUsize::new(EMPTY),
            val: UnsafeCell::new(None),
        });
        // The rest will initially be empty
        for _ in 0..cap {
            buffer.push(Seat::default());
        }

        Shared {
            idx: 0,
            waker_key: NULL_WAKER_KEY,
            inner: Arc::new(Inner {
                buffer: buffer.into(),
                stream: UnsafeCell::new(stream),
                notifier: Arc::new(Notifier::new()),
            }),
        }
    }
}

impl<S: Stream> Shared<S>
where
    S::Item: Clone,
{
    fn take_next(&mut self) -> Option<S::Item> {
        // FIXME: should we stop moving if we reach the end of the
        // stream?
        let (result, idx) = self.inner.take(self.idx);
        self.idx = idx;
        result
    }

    #[inline]
    fn seat(&self) -> &Seat<S::Item> {
        &self.inner.buffer[self.idx]
    }

    fn poll_stream(&mut self) -> Poll<Option<S::Item>> {
        use crate::notifier::State::*;

        match self.inner.notifier.try_start_poll() {
            Idle => {
                // Lock acquired, fall through
                if self.seat().state.load(SeqCst) == FULL {
                    return Poll::Ready(self.take_next());
                }
            }
            Polling => {
                // Another task is currently polling, at this point we just want
                // to ensure that our task handle is currently registered
                return Poll::Pending;
            }
            Complete => {
                let state = &self.seat().state;
                debug_assert!(FULL == state.load(SeqCst));
                return Poll::Ready(self.take_next());
            }
        }

        let waker = waker_ref(&self.inner.notifier);
        let mut cx = Context::from_waker(&waker);

        let _reset = self.inner.notifier.reset_guard();
        let item = {
            // Poll the stream
            let res =
                unsafe { Pin::new_unchecked(&mut *self.inner.stream.get()).poll_next(&mut cx) };
            match res {
                Poll::Pending => {
                    self.inner.notifier.set_idle();
                    drop(_reset);
                    return Poll::Pending;
                }
                Poll::Ready(item) => item,
            }
        };

        let is_end = item.is_none();
        let seat = self.seat();
        unsafe {
            *seat.val.get() = item;
        }
        seat.state.store(FULL, SeqCst);
        // Complete the future
        if is_end {
            self.inner.notifier.finish_poll(_reset);
        } else {
            self.inner.notifier.reset_poll(_reset);
        }
        Poll::Ready(self.take_next())
    }
}

impl<S: Stream> Stream for Shared<S>
where
    S::Item: Clone,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match this.seat().state.load(SeqCst) {
            EMPTY => {
                this.inner.record_waker(&mut this.waker_key, cx);
                this.poll_stream()
            }
            WAITING => {
                this.inner.record_waker(&mut this.waker_key, cx);
                // now check that we are still waiting, just in case it woke up
                // in the meantime
                match this.seat().state.load(SeqCst) {
                    EMPTY => this.poll_stream(),
                    FULL => Poll::Ready(this.take_next()),
                    _ => Poll::Pending,
                }
            }
            FULL => Poll::Ready(this.take_next()),
            _ => unreachable!(),
        }
    }
}

impl<S: Stream> Clone for Shared<S>
where
    S::Item: Clone,
{
    fn clone(&self) -> Self {
        self.seat().refcount.fetch_add(1, SeqCst);
        Shared {
            idx: self.idx,
            waker_key: NULL_WAKER_KEY,
            inner: self.inner.clone(),
        }
    }
}

impl<S: Stream> Drop for Shared<S> {
    fn drop(&mut self) {
        self.inner.notifier.remove(self.waker_key);
        self.inner.drop_ref(self.idx);
    }
}

impl<S: Stream> Inner<S> {
    #[inline]
    fn cap(&self) -> usize {
        self.buffer.len()
    }

    fn prev_idx(&self, idx: usize) -> usize {
        match idx.checked_sub(1) {
            Some(prev) => prev % self.cap(),
            None => self.cap() - 1,
        }
    }
    fn next_idx(&self, idx: usize) -> usize {
        (idx + 1) % self.cap()
    }

    #[inline]
    fn is_first(&self, idx: usize) -> bool {
        self.buffer[self.prev_idx(idx)].state.load(SeqCst) == EMPTY
    }

    /// Notify any waiting tasks that there is space available.
    fn notify_waiting(&self, idx: usize) {
        let seat = &self.buffer[self.prev_idx(idx)];
        if seat.state.compare_and_swap(WAITING, EMPTY, SeqCst) == WAITING {
            ArcWake::wake_by_ref(&self.notifier);
        }
    }

    fn drop_ref(&self, idx: usize) {
        let seat = &self.buffer[idx];
        let refcount = seat.refcount.fetch_sub(1, SeqCst);

        // If we are the last reference to the first seat, then drop the seat.
        if refcount == 1 && self.is_first(idx) {
            // This is safe, because nothing else can access this seat if we are the last
            // thing to use it.
            unsafe { *seat.val.get() = None };
            seat.state.store(EMPTY, SeqCst);
            self.notify_waiting(idx);
        }
    }

    fn record_waker(&self, waker_key: &mut usize, cx: &mut Context<'_>) {
        self.notifier.record_waker(waker_key, cx);
    }
}

impl<S: Stream> Inner<S>
where
    S::Item: Clone,
{
    fn take(&self, idx: usize) -> (Option<S::Item>, usize) {
        let seat = &self.buffer[idx];
        // if we've gotten this far, the seat is already filled.
        debug_assert!(seat.state.load(SeqCst) == FULL);
        let next_idx = self.next_idx(idx);
        let next_refcount = &self.buffer[next_idx].refcount;
        // This is valid because the previous buffer shouldn't be written to
        // until this one is empty.
        let value = if self.is_first(idx) && seat.refcount.load(SeqCst) == 1 {
            // This is safe because there is only a single
            // stream that still has access to this seat, and it
            // is the one currently taking a value out.
            let result = unsafe { (*seat.val.get()).take() };
            next_refcount.fetch_add(1, SeqCst);
            let old = seat.refcount.swap(0, SeqCst);
            assert!(old == 1);
            // Could this be Release instead?
            let old = seat.state.swap(EMPTY, SeqCst);
            assert!(old == FULL);
            self.notify_waiting(idx);
            result
        } else {
            // This is safe because nothing else should write to this until
            // it is the first seat, and it is empty.
            let result = unsafe { (*seat.val.get()).clone() };
            next_refcount.fetch_add(1, SeqCst);
            self.drop_ref(idx);
            result
        };
        (value, next_idx)
    }
}
