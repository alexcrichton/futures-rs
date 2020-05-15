//! Additional combinators for testing async writers.

mod track_closed;

use futures_io::AsyncWrite;

pub use super::limited::Limited;
pub use crate::interleave_pending::InterleavePending;
pub use track_closed::TrackClosed;

/// Additional combinators for testing async writers.
pub trait AsyncWriteTestExt: AsyncWrite {
    /// Introduces an extra [`Poll::Pending`](futures_core::task::Poll::Pending)
    /// in between each operation on the writer.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures::task::Poll;
    /// use futures::io::{AsyncWrite, Cursor};
    /// use futures_test::task::noop_context;
    /// use futures_test::io::AsyncWriteTestExt;
    /// use futures::pin_mut;
    ///
    /// let writer = Cursor::new(vec![0u8; 4].into_boxed_slice()).interleave_pending_write();
    /// pin_mut!(writer);
    ///
    /// let mut cx = noop_context();
    ///
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[1, 2])?, Poll::Pending);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[1, 2])?, Poll::Ready(2));
    /// assert_eq!(&writer.get_ref().get_ref()[..], [1, 2, 0, 0]);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[3, 4])?, Poll::Pending);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[3, 4])?, Poll::Ready(2));
    /// assert_eq!(&writer.get_ref().get_ref()[..], [1, 2, 3, 4]);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[5, 6])?, Poll::Pending);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[5, 6])?, Poll::Ready(0));
    ///
    /// assert_eq!(writer.as_mut().poll_flush(&mut cx)?, Poll::Pending);
    /// assert_eq!(writer.as_mut().poll_flush(&mut cx)?, Poll::Ready(()));
    ///
    /// assert_eq!(writer.as_mut().poll_close(&mut cx)?, Poll::Pending);
    /// assert_eq!(writer.as_mut().poll_close(&mut cx)?, Poll::Ready(()));
    ///
    /// # Ok::<(), std::io::Error>(())
    /// ```
    fn interleave_pending_write(self) -> InterleavePending<Self>
    where
        Self: Sized,
    {
        InterleavePending::new(self)
    }

    /// Limit the number of bytes allowed to be written on each call to `poll_write`.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures::task::Poll;
    /// use futures::io::{AsyncWrite, Cursor};
    /// use futures_test::task::noop_context;
    /// use futures_test::io::AsyncWriteTestExt;
    /// use futures::pin_mut;
    ///
    /// let writer = Cursor::new(vec![0u8; 4].into_boxed_slice()).limited_write(2);
    /// pin_mut!(writer);
    ///
    /// let mut cx = noop_context();
    ///
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[1, 2])?, Poll::Ready(2));
    /// assert_eq!(&writer.get_ref().get_ref()[..], [1, 2, 0, 0]);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[3])?, Poll::Ready(1));
    /// assert_eq!(&writer.get_ref().get_ref()[..], [1, 2, 3, 0]);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[4, 5])?, Poll::Ready(1));
    /// assert_eq!(&writer.get_ref().get_ref()[..], [1, 2, 3, 4]);
    /// assert_eq!(writer.as_mut().poll_write(&mut cx, &[5])?, Poll::Ready(0));
    ///
    /// # Ok::<(), std::io::Error>(())
    /// ```
    fn limited_write(self, limit: usize) -> Limited<Self>
    where
        Self: Sized,
    {
        Limited::new(self, limit)
    }

    /// Track whether this stream has been closed and errors if it is used after closing.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::io::{AsyncWriteExt, Cursor};
    /// use futures_test::io::AsyncWriteTestExt;
    ///
    /// let mut writer = Cursor::new(vec![0u8; 4]).track_closed();
    ///
    /// writer.write_all(&[1, 2]).await?;
    /// assert!(!writer.is_closed());
    /// writer.close().await?;
    /// assert!(writer.is_closed());
    ///
    /// # Ok::<(), std::io::Error>(()) })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::io::{AsyncWriteExt, Cursor};
    /// use futures_test::io::AsyncWriteTestExt;
    ///
    /// let mut writer = Cursor::new(vec![0u8; 4]).track_closed();
    ///
    /// writer.close().await?;
    /// assert!(writer.write_all(&[1, 2]).await.is_err());
    /// # Ok::<(), std::io::Error>(()) })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    fn track_closed(self) -> TrackClosed<Self>
    where
        Self: Sized,
    {
        TrackClosed::new(self)
    }
}

impl<W> AsyncWriteTestExt for W where W: AsyncWrite {}
