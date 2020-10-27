use crate::lock::waiter::WaiterSet;
use futures_core::future::{FusedFuture, Future};
use futures_core::task::{Context, Poll};
use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

#[allow(clippy::identity_op)]
const PHASE: usize = 1 << 0;
const ONE_WRITER: usize = 1 << 1;
const ONE_READER: usize = 1 << 2;
const WRITE_BITS: usize = ONE_WRITER | PHASE;

// Sentinel for when no slot in the `Slab` has been dedicated to this object.
const WAIT_KEY_NONE: usize = usize::max_value();

struct State {
    ins: AtomicUsize,
    out: AtomicUsize,
}

struct AtomicState {
    read: State,
    write: State,
}

impl AtomicState {
    #[inline]
    fn insert_writer(&self) -> usize {
        self.write.ins.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn remove_writer(&self) -> usize {
        self.write.out.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn remove_reader(&self) -> usize {
        self.read.out.fetch_add(ONE_READER, Ordering::SeqCst)
    }

    #[inline]
    fn waiting_writers(&self) -> usize {
        self.write.ins.load(Ordering::Relaxed)
    }

    #[inline]
    fn reserve_reader(&self) -> usize {
        self.read.ins.fetch_add(ONE_READER, Ordering::SeqCst) & WRITE_BITS
    }

    #[inline]
    fn reserve_writer(&self, ticket: usize) -> usize {
        self.read
            .ins
            .fetch_add(ONE_WRITER | (ticket & PHASE), Ordering::SeqCst)
    }

    #[inline]
    fn reserve_transient_writer(&self) -> usize {
        self.read.ins.fetch_add(PHASE, Ordering::SeqCst)
    }

    #[inline]
    fn phase(&self) -> usize {
        self.read.ins.load(Ordering::Relaxed) & WRITE_BITS
    }

    #[inline]
    fn clear_phase(&self) -> usize {
        self.read.ins.fetch_and(!WRITE_BITS, Ordering::Relaxed)
    }

    #[inline]
    fn finished_readers(&self) -> usize {
        self.read.out.load(Ordering::Relaxed)
    }

    #[inline]
    fn finished_writers(&self) -> usize {
        self.write.out.load(Ordering::Relaxed)
    }
}

/// A futures-aware read-write lock.
pub struct RwLock<T: ?Sized> {
    atomic: AtomicState,
    readers: WaiterSet,
    writers: WaiterSet,
    block_read_tickets: StdRwLock<()>,
    block_write_tickets: StdRwLock<()>,
    value: UnsafeCell<T>,
}

impl<T: ?Sized> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLock").finish()
    }
}

impl<T> RwLock<T> {
    /// Creates a new futures-aware read-write lock.
    pub fn new(t: T) -> RwLock<T> {
        RwLock {
            atomic: AtomicState {
                read: State {
                    ins: AtomicUsize::new(0),
                    out: AtomicUsize::new(0),
                },
                write: State {
                    ins: AtomicUsize::new(0),
                    out: AtomicUsize::new(0),
                },
            },
            readers: WaiterSet::new(),
            writers: WaiterSet::new(),
            block_read_tickets: StdRwLock::new(()),
            block_write_tickets: StdRwLock::new(()),
            value: UnsafeCell::new(t),
        }
    }

    /// Consumes the read-write lock, returning the underlying data.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures::lock::RwLock;
    ///
    /// let rwlock = RwLock::new(0);
    /// assert_eq!(rwlock.into_inner(), 0);
    /// ```
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    /// Acquire a read access lock asynchronously.
    ///
    /// This method returns a future that will resolve once all write access
    /// locks have been dropped.
    pub fn read(&self) -> RwLockReadFuture<'_, T> {
        RwLockReadFuture {
            rwlock: Some(self),
            ticket: None,
            wait_key: WAIT_KEY_NONE,
        }
    }

    /// Acquire a write access lock asynchronously.
    ///
    /// This method returns a future that will resolve once all other locks
    /// have been dropped.
    pub fn write(&self) -> RwLockWriteFuture<'_, T> {
        RwLockWriteFuture {
            rwlock: Some(self),
            ticket: None,
            wait_key: WAIT_KEY_NONE,
        }
    }

    /// Attempt to acquire a read access lock synchronously.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let lock = self.block_read_tickets.write().unwrap();
        if self.atomic.phase() == 0 {
            self.atomic.reserve_reader();
            drop(lock);
            self.writers.notify_all();
            Some(RwLockReadGuard { rwlock: self })
        } else {
            drop(lock);
            self.writers.notify_all();
            None
        }
    }

    /// Attempt to acquire a write access lock synchronously.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        let read_lock = self.block_read_tickets.write().unwrap();
        if self.atomic.phase() == 0 {
            let write_lock = self.block_write_tickets.write().unwrap();
            if self.atomic.waiting_writers() == self.atomic.finished_writers()
                && self.atomic.reserve_transient_writer() == self.atomic.finished_readers()
            {
                self.atomic.insert_writer();
                drop(write_lock);
                drop(read_lock);
                self.writers.notify_all();
                Some(RwLockWriteGuard { rwlock: self })
            } else if self.atomic.phase() != 0 {
                self.atomic.clear_phase();
                drop(write_lock);
                drop(read_lock);
                self.writers.notify_all();
                self.readers.notify_all();
                None
            } else {
                drop(write_lock);
                drop(read_lock);
                self.writers.notify_all();
                None
            }
        } else {
            drop(read_lock);
            self.writers.notify_all();
            None
        }
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the lock mutably, no actual locking needs to
    /// take place -- the mutable borrow statically guarantees no locks exist.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::lock::RwLock;
    ///
    /// let mut rwlock = RwLock::new(0);
    /// *rwlock.get_mut() = 10;
    /// assert_eq!(*rwlock.read().await, 10);
    /// # });
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value.get() }
    }
}

#[derive(Debug)]
enum Ticket {
    Read(usize),
    Write(usize),
}

impl Ticket {
    fn value(&self) -> usize {
        match self {
            Ticket::Read(value) => *value,
            Ticket::Write(value) => *value,
        }
    }
}

/// A future which resolves when the target read access lock has been successfully
/// acquired.
pub struct RwLockReadFuture<'a, T: ?Sized> {
    // `None` indicates that the mutex was successfully acquired.
    rwlock: Option<&'a RwLock<T>>,
    ticket: Option<Ticket>,
    wait_key: usize,
}

impl<T: ?Sized> fmt::Debug for RwLockReadFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockReadFuture")
            .field("was_acquired", &self.rwlock.is_none())
            .field("rwlock", &self.rwlock)
            .field("ticket", &self.ticket)
            .field(
                "wait_key",
                &(if self.wait_key == WAIT_KEY_NONE {
                    None
                } else {
                    Some(self.wait_key)
                }),
            )
            .finish()
    }
}

impl<T: ?Sized> FusedFuture for RwLockReadFuture<'_, T> {
    fn is_terminated(&self) -> bool {
        self.rwlock.is_none()
    }
}

impl<'a, T: ?Sized> Future for RwLockReadFuture<'a, T> {
    type Output = RwLockReadGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rwlock = self
            .rwlock
            .expect("polled RwLockReadFuture after completion");

        // The ticket is defined by the write bits stored within the read-in count
        let ticket = self
            .ticket
            .get_or_insert_with(|| Ticket::Read(rwlock.atomic.reserve_reader()))
            .value();

        // Safe to create guard when either there are no writers (ticket == 0) or if
        // at least one of the two write bits change.
        // Writers always wait until the current reader phase completes before acquiring
        // the lock; thus the PHASE bit both maintains the read-write condition and
        // prevents deadlock in the case that this line isn't reached before a writer sets
        // the ONE_WRITER bit.
        if ticket == 0 || ticket != rwlock.atomic.phase() {
            if self.wait_key != WAIT_KEY_NONE {
                rwlock.readers.remove(self.wait_key);
            }
            self.rwlock = None;
            Poll::Ready(RwLockReadGuard { rwlock })
        } else {
            if self.wait_key == WAIT_KEY_NONE {
                self.wait_key = rwlock.readers.insert(cx.waker());
            } else {
                rwlock.readers.register(self.wait_key, cx.waker());
            }
            Poll::Pending
        }
    }
}

impl<T: ?Sized> Drop for RwLockReadFuture<'_, T> {
    fn drop(&mut self) {
        if self.rwlock.is_some() && self.wait_key != WAIT_KEY_NONE {
            panic!("RwLockReadFuture dropped before completion");
        }
    }
}

/// A future which resolves when the target write access lock has been successfully
/// acquired.
pub struct RwLockWriteFuture<'a, T: ?Sized> {
    rwlock: Option<&'a RwLock<T>>,
    ticket: Option<Ticket>,
    wait_key: usize,
}

impl<T: ?Sized> fmt::Debug for RwLockWriteFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockWriteFuture")
            .field("was_acquired", &self.rwlock.is_none())
            .field("rwlock", &self.rwlock)
            .field(
                "wait_key",
                &(if self.wait_key == WAIT_KEY_NONE {
                    None
                } else {
                    Some(self.wait_key)
                }),
            )
            .finish()
    }
}

impl<T: ?Sized> FusedFuture for RwLockWriteFuture<'_, T> {
    fn is_terminated(&self) -> bool {
        self.rwlock.is_none()
    }
}

impl<'a, T: ?Sized> Future for RwLockWriteFuture<'a, T> {
    type Output = RwLockWriteGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let rwlock = self
            .rwlock
            .expect("polled RwLockWriteFuture after completion");

        match self.ticket {
            None => {
                let _write_lock = rwlock.block_write_tickets.read().unwrap();
                let ticket = rwlock.atomic.insert_writer();
                self.ticket = Some(Ticket::Write(ticket));
                if ticket == rwlock.atomic.finished_writers() {
                    // Note that the WRITE_BITS are always cleared at this point.
                    let _read_lock = rwlock.block_read_tickets.read().unwrap();
                    let ticket = rwlock.atomic.reserve_writer(ticket);
                    self.ticket = Some(Ticket::Read(ticket));
                    if ticket == rwlock.atomic.finished_readers() {
                        self.rwlock = None;
                        Poll::Ready(RwLockWriteGuard { rwlock })
                    } else {
                        self.wait_key = rwlock.writers.insert(cx.waker());
                        Poll::Pending
                    }
                } else {
                    self.wait_key = rwlock.writers.insert(cx.waker());
                    Poll::Pending
                }
            }
            Some(Ticket::Write(ticket)) => {
                if ticket == rwlock.atomic.finished_writers() {
                    // Note that the WRITE_BITS are always cleared at this point.
                    let _read_lock = rwlock.block_read_tickets.read().unwrap();
                    let ticket = rwlock.atomic.reserve_writer(ticket);
                    self.ticket = Some(Ticket::Read(ticket));
                    if ticket == rwlock.atomic.finished_readers() {
                        rwlock.writers.remove(self.wait_key);
                        self.rwlock = None;
                        Poll::Ready(RwLockWriteGuard { rwlock })
                    } else {
                        rwlock.writers.register(self.wait_key, cx.waker());
                        Poll::Pending
                    }
                } else {
                    rwlock.writers.register(self.wait_key, cx.waker());
                    Poll::Pending
                }
            }
            Some(Ticket::Read(ticket)) => {
                if ticket == rwlock.atomic.finished_readers() {
                    rwlock.writers.remove(self.wait_key);
                    self.rwlock = None;
                    Poll::Ready(RwLockWriteGuard { rwlock })
                } else {
                    rwlock.writers.register(self.wait_key, cx.waker());
                    Poll::Pending
                }
            }
        }
    }
}

impl<T: ?Sized> Drop for RwLockWriteFuture<'_, T> {
    fn drop(&mut self) {
        if self.rwlock.is_some() && self.wait_key != WAIT_KEY_NONE {
            panic!("RwLockWriteFuture dropped before completion");
        }
    }
}

/// An RAII guard returned by the `read` and `try_read` methods.
/// When all of these structures are dropped (fallen out of scope), the
/// rwlock will be available for write access.
pub struct RwLockReadGuard<'a, T: ?Sized> {
    rwlock: &'a RwLock<T>,
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockReadGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockReadGuard")
            .field("value", &&**self)
            .field("rwlock", &self.rwlock)
            .finish()
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        self.rwlock.atomic.remove_reader();
        self.rwlock.writers.notify_all();
    }
}

impl<T: ?Sized> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.rwlock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockReadGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.rwlock.value.get() }
    }
}

/// An RAII guard returned by the `write` and `try_write` methods.
/// When this structure is dropped (falls out of scope), the rwlock
/// will be available for a future read or write access.
pub struct RwLockWriteGuard<'a, T: ?Sized> {
    rwlock: &'a RwLock<T>,
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLockWriteGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockWriteGuard")
            .field("value", &&**self)
            .field("rwlock", &self.rwlock)
            .finish()
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.rwlock.atomic.remove_writer();
        self.rwlock.atomic.clear_phase();
        self.rwlock.writers.notify_all();
        self.rwlock.readers.notify_all();
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.rwlock.value.get() }
    }
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.rwlock.value.get() }
    }
}

unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}
unsafe impl<T: ?Sized + Sync> Sync for RwLock<T> {}

unsafe impl<T: ?Sized + Send> Send for RwLockReadFuture<'_, T> {}
unsafe impl<T: ?Sized> Sync for RwLockReadFuture<'_, T> {}

unsafe impl<T: ?Sized + Send> Send for RwLockWriteFuture<'_, T> {}
unsafe impl<T: ?Sized> Sync for RwLockWriteFuture<'_, T> {}

unsafe impl<T: ?Sized + Send> Send for RwLockReadGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for RwLockReadGuard<'_, T> {}

unsafe impl<T: ?Sized + Send> Send for RwLockWriteGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for RwLockWriteGuard<'_, T> {}
