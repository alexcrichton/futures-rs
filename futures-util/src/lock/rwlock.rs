use crate::lock::waiter::WaiterSet;
use futures_core::future::{FusedFuture, Future};
use futures_core::task::{Context, Poll};
use std::cell::UnsafeCell;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A futures-aware read-write lock.
pub struct RwLock<T: ?Sized> {
    state: AtomicUsize,
    readers: WaiterSet,
    writers: WaiterSet,
    value: UnsafeCell<T>,
}

impl<T: ?Sized> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::SeqCst);
        f.debug_struct("RwLock")
            .field("is_locked", &((state & IS_LOCKED) != 0))
            .field("readers", &((state & READ_COUNT) >> 1))
            .finish()
    }
}

#[allow(clippy::identity_op)]
const IS_LOCKED: usize = 1 << 0;
const ONE_READER: usize = 1 << 1;
const READ_COUNT: usize = !(ONE_READER - 1);
const MAX_READERS: usize = usize::max_value() >> 1;

impl<T> RwLock<T> {
    /// Creates a new futures-aware read-write lock.
    pub fn new(t: T) -> RwLock<T> {
        RwLock {
            state: AtomicUsize::new(0),
            readers: WaiterSet::new(),
            writers: WaiterSet::new(),
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
    /// Attempt to acquire a lock with shared read access immediately.
    ///
    /// If the lock is currently held by a writer, this will return `None`.
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        let mut state = self.state.load(Ordering::Acquire);

        loop {
            if state & IS_LOCKED != 0 {
                return None;
            }

            if state > MAX_READERS {
                process::abort();
            }

            match self.state.compare_exchange_weak(
                state,
                state + ONE_READER,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(RwLockReadGuard { rwlock: self }),
                Err(s) => state = s,
            }
        }
    }

    /// Attempt to acquire a lock with exclusive write access immediately.
    ///
    /// If there are any other locks, either for read or write access, this
    /// will return `None`.
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        if self.state.compare_and_swap(0, IS_LOCKED, Ordering::SeqCst) == 0 {
            Some(RwLockWriteGuard { rwlock: self })
        } else {
            None
        }
    }

    /// Acquire a read access lock asynchronously.
    ///
    /// This method returns a future that will resolve once all write access
    /// locks have been dropped.
    pub fn read(&self) -> RwLockReadFuture<'_, T> {
        RwLockReadFuture {
            rwlock: Some(self),
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
            wait_key: WAIT_KEY_NONE,
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
    /// assert_eq!(*rwlock.lock().await, 10);
    /// # });
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value.get() }
    }
}

// Sentinel for when no slot in the `Slab` has been dedicated to this object.
const WAIT_KEY_NONE: usize = usize::max_value();

/// A future which resolves when the target read access lock has been successfully
/// acquired.
pub struct RwLockReadFuture<'a, T: ?Sized> {
    // `None` indicates that the mutex was successfully acquired.
    rwlock: Option<&'a RwLock<T>>,
    wait_key: usize,
}

impl<T: ?Sized> fmt::Debug for RwLockReadFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RwLockReadFuture")
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

        if let Some(lock) = rwlock.try_read() {
            if self.wait_key != WAIT_KEY_NONE {
                rwlock.readers.remove(self.wait_key);
            }
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        if self.wait_key == WAIT_KEY_NONE {
            self.wait_key = rwlock.readers.insert(cx.waker());
        } else {
            rwlock.readers.register(self.wait_key, cx.waker());
        }

        // Ensure that we haven't raced `RwLockWriteGuard::drop`'s unlock path by
        // attempting to acquire the lock again.
        if let Some(lock) = rwlock.try_read() {
            rwlock.readers.remove(self.wait_key);
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for RwLockReadFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(rwlock) = self.rwlock {
            if self.wait_key != WAIT_KEY_NONE {
                rwlock.readers.remove(self.wait_key);
            }
        }
    }
}

/// A future which resolves when the target write access lock has been successfully
/// acquired.
pub struct RwLockWriteFuture<'a, T: ?Sized> {
    rwlock: Option<&'a RwLock<T>>,
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

        if let Some(lock) = rwlock.try_write() {
            if self.wait_key != WAIT_KEY_NONE {
                rwlock.writers.remove(self.wait_key);
            }
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        if self.wait_key == WAIT_KEY_NONE {
            self.wait_key = rwlock.writers.insert(cx.waker());
        } else {
            rwlock.writers.register(self.wait_key, cx.waker());
        }

        // Ensure that we haven't raced `RwLockWriteGuard::drop` or
        // `RwLockReadGuard::drop`'s unlock path by attempting to acquire
        // the lock again.
        if let Some(lock) = rwlock.try_write() {
            rwlock.writers.remove(self.wait_key);
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for RwLockWriteFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(rwlock) = self.rwlock {
            if self.wait_key != WAIT_KEY_NONE {
                // This future was dropped before it acquired the rwlock.
                //
                // Remove ourselves from the map, waking up another waiter if we
                // had been awoken to acquire the lock.
                rwlock.writers.cancel(self.wait_key);
            }
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
        let old_state = self.rwlock.state.fetch_sub(ONE_READER, Ordering::SeqCst);
        if old_state & READ_COUNT == ONE_READER {
            self.rwlock.writers.notify_any();
        }
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
        self.rwlock.state.store(0, Ordering::SeqCst);
        if !self.rwlock.readers.notify_all() {
            self.rwlock.writers.notify_any();
        }
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

#[test]
fn test_rwlock_guard_debug_not_recurse() {
    let rwlock = RwLock::new(42);
    let guard = rwlock.try_read().unwrap();
    let _ = format!("{:?}", guard);
    drop(guard);
    let guard = rwlock.try_write().unwrap();
    let _ = format!("{:?}", guard);
}
