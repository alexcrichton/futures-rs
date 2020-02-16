use futures_core::future::{FusedFuture, Future};
use futures_core::task::{Context, Poll, Waker};
use slab::Slab;
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex as StdMutex;
use std::{fmt, mem};

/// A futures-aware read-write lock.
pub struct RwLock<T: ?Sized> {
    state: AtomicUsize,
    read_waiters: StdMutex<Slab<Waiter>>,
    write_waiters: StdMutex<Slab<Waiter>>,
    value: UnsafeCell<T>,
}

impl<T: ?Sized> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::SeqCst);
        f.debug_struct("RwLock")
            .field("is_locked", &((state & IS_LOCKED) != 0))
            .field("has_writers", &((state & HAS_WRITERS) != 0))
            .field("has_readers", &((state & HAS_READERS) != 0))
            .field("active_readers", &((state & READ_COUNT_MASK) >> 3))
            .finish()
    }
}

enum Waiter {
    Waiting(Waker),
    Woken,
}

impl Waiter {
    fn register(&mut self, waker: &Waker) {
        match self {
            Waiter::Waiting(w) if waker.will_wake(w) => {}
            _ => *self = Waiter::Waiting(waker.clone()),
        }
    }

    fn wake(&mut self) {
        match mem::replace(self, Waiter::Woken) {
            Waiter::Waiting(waker) => waker.wake(),
            Waiter::Woken => {}
        }
    }
}

#[allow(clippy::identity_op)]
const IS_LOCKED: usize = 1 << 0;
const HAS_WRITERS: usize = 1 << 1;
const HAS_READERS: usize = 1 << 2;
const ONE_READER: usize = 1 << 3;
const READ_COUNT_MASK: usize = !(ONE_READER - 1);
const MAX_READERS: usize = usize::max_value() >> 3;

impl<T> RwLock<T> {
    /// Creates a new futures-aware read-write lock.
    pub fn new(t: T) -> RwLock<T> {
        RwLock {
            state: AtomicUsize::new(0),
            read_waiters: StdMutex::new(Slab::new()),
            write_waiters: StdMutex::new(Slab::new()),
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

    fn remove_reader(&self, wait_key: usize) {
        if wait_key != WAIT_KEY_NONE {
            let mut readers = self.read_waiters.lock().unwrap();
            // No need to check whether another waiter needs to be
            // woken up since no other readers depend on this.
            readers.remove(wait_key);
            if readers.is_empty() {
                self.state.fetch_and(!HAS_READERS, Ordering::Relaxed);
            }
        }
    }

    fn remove_writer(&self, wait_key: usize, wake_another: bool) {
        if wait_key != WAIT_KEY_NONE {
            let mut writers = self.write_waiters.lock().unwrap();
            match writers.remove(wait_key) {
                Waiter::Waiting(_) => {}
                Waiter::Woken => {
                    // We were awoken, but then dropped before we could
                    // wake up to acquire the lock. Wake up another
                    // waiter.
                    if wake_another {
                        if let Some((_, waiter)) = writers.iter_mut().next() {
                            waiter.wake();
                        }
                    }
                }
            }
            if writers.is_empty() {
                self.state.fetch_and(!HAS_WRITERS, Ordering::Relaxed);
            }
        }
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
            rwlock.remove_reader(self.wait_key);
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        {
            let mut readers = rwlock.read_waiters.lock().unwrap();
            if self.wait_key == WAIT_KEY_NONE {
                self.wait_key = readers.insert(Waiter::Waiting(cx.waker().clone()));
                if readers.len() == 1 {
                    rwlock.state.fetch_or(HAS_READERS, Ordering::Relaxed);
                }
            } else {
                readers[self.wait_key].register(cx.waker());
            }
        }

        // Ensure that we haven't raced `RwLockWriteGuard::drop`'s unlock path by
        // attempting to acquire the lock again.
        if let Some(lock) = rwlock.try_read() {
            rwlock.remove_reader(self.wait_key);
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for RwLockReadFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(rwlock) = self.rwlock {
            rwlock.remove_reader(self.wait_key);
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
            rwlock.remove_writer(self.wait_key, false);
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        {
            let mut writers = rwlock.write_waiters.lock().unwrap();
            if self.wait_key == WAIT_KEY_NONE {
                self.wait_key = writers.insert(Waiter::Waiting(cx.waker().clone()));
                if writers.len() == 1 {
                    rwlock.state.fetch_or(HAS_WRITERS, Ordering::Relaxed);
                }
            } else {
                writers[self.wait_key].register(cx.waker());
            }
        }

        // Ensure that we haven't raced `RwLockWriteGuard::drop` or
        // `RwLockReadGuard::drop`'s unlock path by attempting to acquire
        // the lock again.
        if let Some(lock) = rwlock.try_write() {
            rwlock.remove_writer(self.wait_key, false);
            self.rwlock = None;
            return Poll::Ready(lock);
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for RwLockWriteFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(rwlock) = self.rwlock {
            // This future was dropped before it acquired the rwlock.
            //
            // Remove ourselves from the map, waking up another waiter if we
            // had been awoken to acquire the lock.
            rwlock.remove_writer(self.wait_key, true);
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
        if old_state & READ_COUNT_MASK == ONE_READER && old_state & HAS_WRITERS != 0 {
            let mut writers = self.rwlock.write_waiters.lock().unwrap();
            if let Some((_, waiter)) = writers.iter_mut().next() {
                waiter.wake();
            }
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
        let old_state = self.rwlock.state.fetch_and(!IS_LOCKED, Ordering::AcqRel);
        match (old_state & HAS_WRITERS, old_state & HAS_READERS) {
            (0, 0) => {}
            (0, _) => {
                let mut readers = self.rwlock.read_waiters.lock().unwrap();
                for (_, waiter) in readers.iter_mut() {
                    waiter.wake();
                }
            }
            _ => {
                let mut writers = self.rwlock.write_waiters.lock().unwrap();
                if let Some((_, waiter)) = writers.iter_mut().next() {
                    waiter.wake();
                }
            }
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
