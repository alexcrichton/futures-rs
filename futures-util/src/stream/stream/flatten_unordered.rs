use crate::stream::FuturesUnordered;
use core::fmt;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::sync::atomic::{Ordering, AtomicU8};
use alloc::sync::Arc;
use futures_core::future::Future;
use futures_core::stream::FusedStream;
use futures_core::stream::Stream;
use futures_core::task::{Context, Poll, Waker};
#[cfg(feature = "sink")]
use futures_sink::Sink;
use futures_task::{waker, ArcWake};
use core::cell::UnsafeCell;
use pin_project::pin_project;

/// Indicates that there is nothing to poll and stream isn't being polled at
/// the moment.
const NONE: u8 = 0;

/// Indicates that `inner_streams` need to be polled.
const NEED_TO_POLL_INNER_STREAMS: u8 = 1;

/// Indicates that `stream` needs to be polled.
const NEED_TO_POLL_STREAM: u8 = 0b10;

/// Indicates that it needs to poll something.
const NEED_TO_POLL_ALL: u8 = NEED_TO_POLL_INNER_STREAMS | NEED_TO_POLL_STREAM;

/// Indicates that current stream is being polled at the moment.
const POLLING: u8 = 0b100;

// Indicates that it already called one of wakers.
const WOKEN: u8 = 0b1000;

/// Determines what needs to be polled, and is stream being polled at the
/// moment or not.
#[derive(Clone, Debug)]
struct SharedPollState {
    state: Arc<AtomicU8>,
}

impl SharedPollState {
    /// Constructs new `SharedPollState` with given state.
    fn new(state: u8) -> SharedPollState {
        SharedPollState {
            state: Arc::new(AtomicU8::new(state)),
        }
    }

    /// Swaps state with `POLLING`, returning previous state.
    fn begin_polling(&self) -> u8 {
        self.state.swap(POLLING, Ordering::SeqCst)
    }

    /// Performs bitwise or with `to_poll` and given state, returning
    /// previous state.
    fn set_or(&self, to_poll: u8) -> u8 {
        self.state.fetch_or(to_poll, Ordering::SeqCst)
    }

    /// Performs bitwise or with `to_poll` and current state, stores result
    /// with non-`POLLING` state, and returns disjunction result.
    fn end_polling(&self, to_poll: u8) -> u8 {
        let poll_state = to_poll | self.state.swap(to_poll & !POLLING & !WOKEN, Ordering::SeqCst);

        if to_poll & NEED_TO_POLL_ALL != poll_state & NEED_TO_POLL_ALL {
            self.state.swap(poll_state & !POLLING & !WOKEN, Ordering::SeqCst);
        }

        poll_state
    }
}

/// Waker which will update `poll_state` with `need_to_poll` value on
/// `wake_by_ref` call and then, if there is a need, call `inner_waker`.
struct PollWaker {
    inner_waker: UnsafeCell<Option<Waker>>,
    poll_state: SharedPollState,
    need_to_poll: u8,
}

unsafe impl Send for PollWaker {}

unsafe impl Sync for PollWaker {}

impl PollWaker {
    /// Replaces given waker's inner_waker for polling stream/futures which will 
    /// update poll state on `wake_by_ref` call. Use only if you need several
    /// contexts.
    /// 
    /// ## Safety
    /// 
    /// This function will modify waker's `inner_waker` via `UnsafeCell`, so
    /// it should be used only during `POLLING` phase.
    unsafe fn replace_waker(self_arc: &mut Arc<Self>, ctx: &Context<'_>) -> Waker {
        *self_arc.inner_waker.get() = ctx.waker().clone().into();
        waker(self_arc.clone())
    }
}

impl ArcWake for PollWaker {
    fn wake_by_ref(self_arc: &Arc<Self>) {
        let poll_state_value = self_arc.poll_state.set_or(self_arc.need_to_poll);
        // Only call waker if stream isn't being polled because it will be called
        // at the end of polling if state was changed.
        if poll_state_value & (POLLING | WOKEN) == NONE {
            if let Some(Some(inner_waker)) = unsafe { self_arc.inner_waker.get().as_ref() } {
                self_arc.poll_state.set_or(WOKEN);
                inner_waker.wake_by_ref();
            }
        }
    }
}

/// Future which contains optional stream. If it's `Some`, it will attempt
/// to call `poll_next` on it, returning `Some((item, next_item_fut))` in
/// case of `Poll::Ready(Some(...))` or `None` in case of `Poll::Ready(None)`.
/// If `poll_next` will return `Poll::Pending`, it will be forwared to
/// the future, and current task will be notified by waker.
#[pin_project]
#[must_use = "futures do nothing unless you `.await` or poll them"]
struct PollStreamFut<St> {
    #[pin]
    stream: Option<St>,
}

impl<St> PollStreamFut<St> {
    /// Constructs new `PollStreamFut` using given `stream`.
    fn new(stream: impl Into<Option<St>>) -> Self {
        Self {
            stream: stream.into(),
        }
    }
}

impl<St: Stream> Future for PollStreamFut<St> {
    type Output = Option<(St::Item, PollStreamFut<St>)>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut stream = self.project().stream;

        let item = if let Some(stream) = stream.as_mut().as_pin_mut() {
            ready!(stream.poll_next(ctx))
        } else {
            None
        };

        Poll::Ready(item.map(|item| {
            (
                item,
                PollStreamFut::new(unsafe { stream.get_unchecked_mut().take() }),
            )
        }))
    }
}

/// Stream for the [`flatten_unordered`](super::StreamExt::flatten_unordered)
/// method.
#[pin_project]
#[must_use = "streams do nothing unless polled"]
pub struct FlattenUnordered<St, U> {
    #[pin]
    inner_streams: FuturesUnordered<PollStreamFut<U>>,
    #[pin]
    stream: St,
    poll_state: SharedPollState,
    limit: Option<NonZeroUsize>,
    is_stream_done: bool,
    inner_streams_waker: Arc<PollWaker>,
    stream_waker: Arc<PollWaker>
}

impl<St> fmt::Debug for FlattenUnordered<St, St::Item>
where
    St: Stream + fmt::Debug,
    St::Item: Stream + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlattenUnordered")
            .field("poll_state", &self.poll_state)
            .field("inner_streams", &self.inner_streams)
            .field("limit", &self.limit)
            .field("stream", &self.stream)
            .field("is_stream_done", &self.is_stream_done)
            .finish()
    }
}

impl<St> FlattenUnordered<St, St::Item>
where
    St: Stream,
    St::Item: Stream,
{
    pub(super) fn new(stream: St, limit: Option<usize>) -> FlattenUnordered<St, St::Item> {
        let poll_state = SharedPollState::new(NEED_TO_POLL_STREAM);

        FlattenUnordered {
            inner_streams: FuturesUnordered::new(),
            stream,
            is_stream_done: false,
            limit: limit.and_then(NonZeroUsize::new),
            inner_streams_waker: Arc::new(PollWaker {
                inner_waker: UnsafeCell::new(None),
                poll_state: poll_state.clone(),
                need_to_poll: NEED_TO_POLL_INNER_STREAMS,
            }),
            stream_waker: Arc::new(PollWaker {
                inner_waker: UnsafeCell::new(None),
                poll_state: poll_state.clone(),
                need_to_poll: NEED_TO_POLL_STREAM,
            }),
            poll_state
        }
    }

    /// Checks if current `inner_streams` size is less than optional limit.
    fn is_exceeded_limit(&self) -> bool {
        self.limit
            .map(|limit| self.inner_streams.len() >= limit.get())
            .unwrap_or(false)
    }

    delegate_access_inner!(stream, St, ());
}

impl<St> FusedStream for FlattenUnordered<St, St::Item>
where
    St: FusedStream,
    St::Item: FusedStream,
{
    fn is_terminated(&self) -> bool {
        self.inner_streams.is_empty() && self.stream.is_terminated()
    }
}

impl<St> Stream for FlattenUnordered<St, St::Item>
where
    St: Stream,
    St::Item: Stream,
{
    type Item = <St::Item as Stream>::Item;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut next_item = None;
        let mut need_to_poll_next = NONE;
        let mut stream_will_be_woken = self.is_exceeded_limit();
        let mut inner_streams_will_be_woken = false;
        
        let mut this = self.project();

        let mut poll_state_value = this.poll_state.begin_polling();

        if poll_state_value & NEED_TO_POLL_ALL == NONE {
            poll_state_value = 
                NEED_TO_POLL_STREAM | if this.inner_streams.is_empty() { NONE } else { NEED_TO_POLL_INNER_STREAMS };
        }

        let mut polling_with_two_wakers = 
            poll_state_value & NEED_TO_POLL_ALL == NEED_TO_POLL_ALL && !stream_will_be_woken;

        if poll_state_value & NEED_TO_POLL_STREAM != NONE {
            if !stream_will_be_woken {
                match if polling_with_two_wakers {
                    // Safety: now state is `POLLING`.
                    let waker = unsafe { PollWaker::replace_waker(this.stream_waker, ctx) };
                    let mut ctx = Context::from_waker(&waker);
                    this.stream.as_mut().poll_next(&mut ctx)
                } else {
                    this.stream.as_mut().poll_next(ctx)
                } {
                    Poll::Ready(Some(inner_stream)) => {
                        this.inner_streams.as_mut().push(PollStreamFut::new(inner_stream));
                        need_to_poll_next |= NEED_TO_POLL_STREAM;
                        // Polling inner streams in current iteration with the same context
                        // is ok because we already received `Poll::Ready` from
                        // stream
                        poll_state_value |= NEED_TO_POLL_INNER_STREAMS;
                        polling_with_two_wakers = false;
                        *this.is_stream_done = false;
                    }
                    Poll::Ready(None) => {
                        // Polling inner streams in current iteration with the same context
                        // is ok because we already received `Poll::Ready` from
                        // stream
                        polling_with_two_wakers = false;
                        *this.is_stream_done = true;
                    }
                    Poll::Pending => {
                        stream_will_be_woken = true;
                        if !polling_with_two_wakers {
                            need_to_poll_next |= NEED_TO_POLL_STREAM;
                        }
                        *this.is_stream_done = false;
                    }
                }
            } else {
                need_to_poll_next |= NEED_TO_POLL_STREAM;
            }
        }

        if poll_state_value & NEED_TO_POLL_INNER_STREAMS != NONE {
            match if polling_with_two_wakers {
                // Safety: now state is `POLLING`.
                let waker = unsafe { PollWaker::replace_waker(this.inner_streams_waker, ctx) };
                let mut ctx = Context::from_waker(&waker);
                this.inner_streams.as_mut().poll_next(&mut ctx)
            } else {
                this.inner_streams.as_mut().poll_next(ctx)
            } {
                Poll::Ready(Some(Some((item, next_item_fut)))) => {
                    this.inner_streams.as_mut().push(next_item_fut);
                    next_item = Some(item);
                    need_to_poll_next |= NEED_TO_POLL_INNER_STREAMS;
                }
                Poll::Ready(Some(None)) => {
                    need_to_poll_next |= NEED_TO_POLL_INNER_STREAMS;
                }
                Poll::Pending => {
                    inner_streams_will_be_woken = true;
                    if !polling_with_two_wakers {
                        need_to_poll_next |= NEED_TO_POLL_INNER_STREAMS;
                    }
                }
                Poll::Ready(None) => {
                    need_to_poll_next &= !NEED_TO_POLL_INNER_STREAMS;
                }
            }
        }

        let poll_state_value = this.poll_state.end_polling(need_to_poll_next);

        let is_done = *this.is_stream_done && this.inner_streams.is_empty();

        if !is_done && poll_state_value & WOKEN == NONE && poll_state_value & NEED_TO_POLL_ALL != NONE
            && (polling_with_two_wakers
                || poll_state_value & NEED_TO_POLL_INNER_STREAMS != NONE && !inner_streams_will_be_woken
                    || poll_state_value & NEED_TO_POLL_STREAM != NONE && !stream_will_be_woken)
        {
            ctx.waker().wake_by_ref();
        }

        if next_item.is_some() || is_done {
            Poll::Ready(next_item)
        } else {
            Poll::Pending
        }
    }
}

// Forwarding impl of Sink from the underlying stream
#[cfg(feature = "sink")]
impl<S, Item> Sink<Item> for FlattenUnordered<S, S::Item>
where
    S: Stream + Sink<Item>,
    S::Item: Stream,
{
    type Error = S::Error;

    delegate_sink!(stream, Item);
}