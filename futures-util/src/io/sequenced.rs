use core::mem;
use core::pin::Pin;

use futures_channel::oneshot;
use futures_core::ready;
use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use futures_task::{Context, Poll};

use crate::future::poll_fn;
use crate::FutureExt;

#[derive(Debug)]
enum SequencedState<T> {
    Active { value: T },
    Waiting { receiver: oneshot::Receiver<Self> },
}

/// Allows multiple asynchronous tasks to access the same reader or writer concurrently
/// without conflicting.
/// The `split_seq` and `split_seq_rev` methods produce a new instance of the type such that
/// all I/O operations on one instance are sequenced before all I/O operations on the other.
///
/// When one task has finished with the reader/writer it should call `release`, which will
/// unblock operations on the task with the other instance. This does not happen automatically
/// on drop in order to ensure that the reader/writer is left in a valid state for the next user.
/// This serves a similar purpose to mutex poisoning.
///
/// The `Sequenced<T>` can be split as many times as necessary, and it is valid to call
/// `release()` at any time, although no further operations can be performed via a released
/// instance. If this type is dropped without calling `release()`, then it will be as though
/// the reader/writer was closed for all those sequenced after this one.
///
/// As only one task has access to the reader/writer at once, no additional synchronization
/// is necessary, and so this wrapper adds very little overhead. What synchronization does
/// occur only needs to happen when an instance is released, in order to send its state to
/// the next instance in the sequence.
///
/// Merging can be achieved by simply releasing one of the two instances, and then using the
/// other one as normal. It does not matter Which one is released.
#[derive(Debug)]
pub struct Sequenced<T> {
    parent: Option<oneshot::Sender<SequencedState<T>>>,
    state: Option<SequencedState<T>>,
}

impl<T> Sequenced<T> {
    /// Constructs a new sequenced reader/writer
    pub fn new(value: T) -> Self {
        Self {
            parent: None,
            state: Some(SequencedState::Active { value }),
        }
    }
    /// Splits this reader/writer into two such that the returned instance is sequenced before this one.
    pub fn split_seq(&mut self) -> Self {
        let (sender, receiver) = oneshot::channel();
        let state = mem::replace(&mut self.state, Some(SequencedState::Waiting { receiver }));
        Self {
            parent: Some(sender),
            state,
        }
    }
    /// Splits this reader/writer into two such that the returned instance is sequenced after this one.
    pub fn split_seq_rev(&mut self) -> Self {
        let other = self.split_seq();
        mem::replace(self, other)
    }

    /// Release this reader/writer immediately, allowing instances sequenced after this one to proceed.
    pub fn release(&mut self) {
        if let (Some(state), Some(parent)) = (self.state.take(), self.parent.take()) {
            let _ = parent.send(state);
        }
    }
    fn resolve(&mut self, cx: &mut Context<'_>) -> Poll<Option<&mut T>> {
        while let Some(SequencedState::Waiting { receiver }) = &mut self.state {
            self.state = ready!(receiver.poll_unpin(cx)).ok();
        }
        Poll::Ready(match &mut self.state {
            Some(SequencedState::Active { value }) => Some(value),
            Some(SequencedState::Waiting { .. }) => unreachable!(),
            None => None,
        })
    }
    /// Attempt to take the inner reader/writer. This will require waiting until prior instances
    /// have been released, and will fail with `None` if any were dropped without being released,
    /// or were themselves taken.
    /// Instances sequenced after this one will see the reader/writer be closed.
    pub fn poll_take(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        ready!(self.as_mut().resolve(cx));
        if let Some(SequencedState::Active { value }) = self.as_mut().state.take() {
            Poll::Ready(Some(value))
        } else {
            Poll::Ready(None)
        }
    }
    /// Attempt to take the inner reader/writer. This will require waiting until prior instances
    /// have been released, and will fail with `None` if any were dropped without being released,
    /// or were themselves taken.
    /// Instances sequenced after this one will see the reader/writer be closed.
    pub async fn take(&mut self) -> Option<T> {
        poll_fn(|cx| Pin::new(&mut *self).poll_take(cx)).await
    }
}

impl<T> Unpin for Sequenced<T> {}

impl<T: Unpin + AsyncRead> AsyncRead for Sequenced<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<futures_io::Result<usize>> {
        if let Some(inner) = ready!(self.get_mut().resolve(cx)) {
            Pin::new(inner).poll_read(cx, buf)
        } else {
            Poll::Ready(Ok(0))
        }
    }
}

impl<T: Unpin + AsyncBufRead> AsyncBufRead for Sequenced<T> {
    fn poll_fill_buf(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<futures_io::Result<&[u8]>> {
        if let Some(inner) = ready!(self.get_mut().resolve(cx)) {
            Pin::new(inner).poll_fill_buf(cx)
        } else {
            Poll::Ready(Ok(&[]))
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        if let Some(SequencedState::Active { value }) = &mut self.get_mut().state {
            Pin::new(value).consume(amt);
        } else {
            panic!("Called `consume()` without having filled the buffer")
        }
    }
}

impl<T: Unpin + AsyncWrite> AsyncWrite for Sequenced<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<futures_io::Result<usize>> {
        if let Some(inner) = ready!(self.get_mut().resolve(cx)) {
            Pin::new(inner).poll_write(cx, buf)
        } else {
            Poll::Ready(Ok(0))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<futures_io::Result<()>> {
        if let Some(inner) = ready!(self.get_mut().resolve(cx)) {
            Pin::new(inner).poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<futures_io::Result<()>> {
        if let Some(inner) = ready!(self.get_mut().resolve(cx)) {
            Pin::new(inner).poll_close(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
