use futures_core::future::{FusedFuture, Future};
use futures_core::task::{Context, Poll, Waker};
use slab::Slab;
use std::{fmt, mem};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::VecDeque;

struct Inner {
    slab: Slab<Waiter>,
    queue: VecDeque<usize>,
}

/// A futures-aware mutex.
///
/// # Fairness
///
/// This mutex provides no fairness guarantees. Tasks may not acquire the mutex
/// in the order that they requested the lock, and it's possible for a single task
/// which repeatedly takes the lock to starve other tasks, which may be left waiting
/// indefinitely.
pub struct Mutex<T: ?Sized> {
    state: AtomicUsize,
    waiters: StdMutex<Inner>,
    value: UnsafeCell<T>,
}

impl<T: ?Sized> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.load(Ordering::SeqCst);
        f.debug_struct("Mutex")
            .field("is_locked", &((state & IS_LOCKED) != 0))
            .field("has_waiters", &((state & HAS_WAITERS) != 0))
            .finish()
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(t: T) -> Self {
        Self::new(t)
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

enum Waiter {
    Waiting(Waker),
    Woken,
}

impl Waiter {
    fn register(&mut self, waker: &Waker) -> bool {
        match self {
            Self::Waiting(w) if waker.will_wake(w) => false,
            Self::Waiting(_) => {
                *self = Self::Waiting(waker.clone());
                false
            }
            Self::Woken => {
                *self = Self::Waiting(waker.clone());
                true
            }
        }
    }

    fn wake(&mut self) {
        match mem::replace(self, Self::Woken) {
            Self::Waiting(waker) => waker.wake(),
            Self::Woken => {}
        }
    }
}

#[allow(clippy::identity_op)] // https://github.com/rust-lang/rust-clippy/issues/3445
const IS_LOCKED: usize = 1 << 0;
const HAS_WAITERS: usize = 1 << 1;

impl<T> Mutex<T> {
    /// Creates a new futures-aware mutex.
    pub fn new(t: T) -> Self {
        Self {
            state: AtomicUsize::new(0),
            waiters: StdMutex::new(Inner { slab: Slab::new(), queue: VecDeque::new() }),
            value: UnsafeCell::new(t),
        }
    }

    /// Consumes this mutex, returning the underlying data.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures::lock::Mutex;
    ///
    /// let mutex = Mutex::new(0);
    /// assert_eq!(mutex.into_inner(), 0);
    /// ```
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Attempt to acquire the lock immediately.
    ///
    /// If the lock is currently held, this will return `None`.
    pub fn try_lock(&self) -> Option<MutexGuard<'_, T>> {
        let old_state = self.state.fetch_or(IS_LOCKED, Ordering::Acquire);
        if (old_state & IS_LOCKED) == 0 {
            Some(MutexGuard { mutex: self })
        } else {
            None
        }
    }

    /// Acquire the lock asynchronously.
    ///
    /// This method returns a future that will resolve once the lock has been
    /// successfully acquired.
    pub fn lock(&self) -> MutexLockFuture<'_, T> {
        MutexLockFuture {
            mutex: Some(self),
            wait_key: WAIT_KEY_NONE,
        }
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// Since this call borrows the `Mutex` mutably, no actual locking needs to
    /// take place -- the mutable borrow statically guarantees no locks exist.
    ///
    /// # Examples
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::lock::Mutex;
    ///
    /// let mut mutex = Mutex::new(0);
    /// *mutex.get_mut() = 10;
    /// assert_eq!(*mutex.lock().await, 10);
    /// # });
    /// ```
    pub fn get_mut(&mut self) -> &mut T {
        // We know statically that there are no other references to `self`, so
        // there's no need to lock the inner mutex.
        unsafe { &mut *self.value.get() }
    }

    fn remove_waker(&self, wait_key: usize, wake_another: bool) {
        if wait_key != WAIT_KEY_NONE {
            let mut waiters = self.waiters.lock().unwrap();
            match waiters.slab.remove(wait_key) {
                Waiter::Waiting(_) => {}
                Waiter::Woken => {
                    // We were awoken, but then dropped before we could
                    // wake up to acquire the lock. Wake up another
                    // waiter.
                    if wake_another {
                        while let Some(other_key) = waiters.queue.pop_front() {
                            if waiters.slab.contains(other_key) {
                                waiters.slab.remove(other_key).wake();
                                break;
                            }
                        }
                    }
                }
            }
            if waiters.slab.is_empty() {
                self.state.fetch_and(!HAS_WAITERS, Ordering::Relaxed); // released by mutex unlock
            }
        }
    }

    // Unlocks the mutex. Called by MutexGuard and MappedMutexGuard when they are
    // dropped.
    fn unlock(&self) {
        let old_state = self.state.fetch_and(!IS_LOCKED, Ordering::AcqRel);
        if (old_state & HAS_WAITERS) != 0 {
            let mut waiters = self.waiters.lock().unwrap();
            while let Some(wait_key) = waiters.queue.pop_front() {
                if waiters.slab.contains(wait_key) {
                    waiters.slab[wait_key].wake();
                    break;
                }
            }
        }
    }
}

// Sentinel for when no slot in the `Slab` has been dedicated to this object.
const WAIT_KEY_NONE: usize = usize::max_value();

/// A future which resolves when the target mutex has been successfully acquired.
pub struct MutexLockFuture<'a, T: ?Sized> {
    // `None` indicates that the mutex was successfully acquired.
    mutex: Option<&'a Mutex<T>>,
    wait_key: usize,
}

impl<T: ?Sized> fmt::Debug for MutexLockFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MutexLockFuture")
            .field("was_acquired", &self.mutex.is_none())
            .field("mutex", &self.mutex)
            .field("wait_key", &(
                if self.wait_key == WAIT_KEY_NONE {
                    None
                } else {
                    Some(self.wait_key)
                }
            ))
            .finish()
    }
}

impl<T: ?Sized> FusedFuture for MutexLockFuture<'_, T> {
    fn is_terminated(&self) -> bool {
        self.mutex.is_none()
    }
}

impl<'a, T: ?Sized> Future for MutexLockFuture<'a, T> {
    type Output = MutexGuard<'a, T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mutex = self.mutex.expect("polled MutexLockFuture after completion");

        if let Some(lock) = mutex.try_lock() {
            mutex.remove_waker(self.wait_key, false);
            self.mutex = None;
            return Poll::Ready(lock);
        }

        {
            let mut waiters = mutex.waiters.lock().unwrap();
            if self.wait_key == WAIT_KEY_NONE {
                self.wait_key = waiters.slab.insert(Waiter::Waiting(cx.waker().clone()));
                waiters.queue.push_back(self.wait_key);
                if waiters.slab.len() == 1 {
                    mutex.state.fetch_or(HAS_WAITERS, Ordering::Relaxed); // released by mutex unlock
                }
            } else {
                if waiters.slab[self.wait_key].register(cx.waker()) {
                    waiters.queue.push_back(self.wait_key);
                }
            }
        }

        // Ensure that we haven't raced `MutexGuard::drop`'s unlock path by
        // attempting to acquire the lock again.
        if let Some(lock) = mutex.try_lock() {
            mutex.remove_waker(self.wait_key, false);
            self.mutex = None;
            return Poll::Ready(lock);
        }

        Poll::Pending
    }
}

impl<T: ?Sized> Drop for MutexLockFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(mutex) = self.mutex {
            // This future was dropped before it acquired the mutex.
            //
            // Remove ourselves from the map, waking up another waiter if we
            // had been awoken to acquire the lock.
            mutex.remove_waker(self.wait_key, true);
        }
    }
}

/// An RAII guard returned by the `lock` and `try_lock` methods.
/// When this structure is dropped (falls out of scope), the lock will be
/// unlocked.
pub struct MutexGuard<'a, T: ?Sized> {
    mutex: &'a Mutex<T>,
}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    /// Returns a locked view over a portion of the locked data.
    ///
    /// # Example
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::lock::{Mutex, MutexGuard};
    ///
    /// let data = Mutex::new(Some("value".to_string()));
    /// {
    ///     let locked_str = MutexGuard::map(data.lock().await, |opt| opt.as_mut().unwrap());
    ///     assert_eq!(&*locked_str, "value");
    /// }
    /// # });
    /// ```
    #[inline]
    pub fn map<U: ?Sized, F>(this: Self, f: F) -> MappedMutexGuard<'a, T, U>
        where
            F: FnOnce(&mut T) -> &mut U,
    {
        let mutex = this.mutex;
        let value = f(unsafe { &mut *this.mutex.value.get() });
        // Don't run the `drop` method for MutexGuard. The ownership of the underlying
        // locked state is being moved to the returned MappedMutexGuard.
        mem::forget(this);
        MappedMutexGuard { mutex, value, _marker: PhantomData }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MutexGuard")
            .field("value", &&**self)
            .field("mutex", &self.mutex)
            .finish()
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        self.mutex.unlock()
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.mutex.value.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.value.get() }
    }
}

/// An RAII guard returned by the `MutexGuard::map` and `MappedMutexGuard::map` methods.
/// When this structure is dropped (falls out of scope), the lock will be unlocked.
pub struct MappedMutexGuard<'a, T: ?Sized, U: ?Sized> {
    mutex: &'a Mutex<T>,
    value: *mut U,
    _marker: PhantomData<&'a mut U>,
}

impl<'a, T: ?Sized, U: ?Sized> MappedMutexGuard<'a, T, U> {
    /// Returns a locked view over a portion of the locked data.
    ///
    /// # Example
    ///
    /// ```
    /// # futures::executor::block_on(async {
    /// use futures::lock::{MappedMutexGuard, Mutex, MutexGuard};
    ///
    /// let data = Mutex::new(Some("value".to_string()));
    /// {
    ///     let locked_str = MutexGuard::map(data.lock().await, |opt| opt.as_mut().unwrap());
    ///     let locked_char = MappedMutexGuard::map(locked_str, |s| s.get_mut(0..1).unwrap());
    ///     assert_eq!(&*locked_char, "v");
    /// }
    /// # });
    /// ```
    #[inline]
    pub fn map<V: ?Sized, F>(this: Self, f: F) -> MappedMutexGuard<'a, T, V>
        where
            F: FnOnce(&mut U) -> &mut V,
    {
        let mutex = this.mutex;
        let value = f(unsafe { &mut *this.value });
        // Don't run the `drop` method for MappedMutexGuard. The ownership of the underlying
        // locked state is being moved to the returned MappedMutexGuard.
        mem::forget(this);
        MappedMutexGuard { mutex, value, _marker: PhantomData }
    }
}

impl<T: ?Sized, U: ?Sized + fmt::Debug> fmt::Debug for MappedMutexGuard<'_, T, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MappedMutexGuard")
            .field("value", &&**self)
            .field("mutex", &self.mutex)
            .finish()
    }
}

impl<T: ?Sized, U: ?Sized> Drop for MappedMutexGuard<'_, T, U> {
    fn drop(&mut self) {
        self.mutex.unlock()
    }
}

impl<T: ?Sized, U: ?Sized> Deref for MappedMutexGuard<'_, T, U> {
    type Target = U;
    fn deref(&self) -> &U {
        unsafe { &*self.value }
    }
}

impl<T: ?Sized, U: ?Sized> DerefMut for MappedMutexGuard<'_, T, U> {
    fn deref_mut(&mut self) -> &mut U {
        unsafe { &mut *self.value }
    }
}

// Mutexes can be moved freely between threads and acquired on any thread so long
// as the inner value can be safely sent between threads.
unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}

unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

// It's safe to switch which thread the acquire is being attempted on so long as
// `T` can be accessed on that thread.
unsafe impl<T: ?Sized + Send> Send for MutexLockFuture<'_, T> {}

// doesn't have any interesting `&self` methods (only Debug)
unsafe impl<T: ?Sized> Sync for MutexLockFuture<'_, T> {}

// Safe to send since we don't track any thread-specific details-- the inner
// lock is essentially spinlock-equivalent (attempt to flip an atomic bool)
unsafe impl<T: ?Sized + Send> Send for MutexGuard<'_, T> {}

unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}

unsafe impl<T: ?Sized + Send, U: ?Sized + Send> Send for MappedMutexGuard<'_, T, U> {}

unsafe impl<T: ?Sized + Sync, U: ?Sized + Sync> Sync for MappedMutexGuard<'_, T, U> {}

#[test]
fn test_mutex_guard_debug_not_recurse() {
    let mutex = Mutex::new(42);
    let guard = mutex.try_lock().unwrap();
    let _ = format!("{:?}", guard);
    let guard = MutexGuard::map(guard, |n| n);
    let _ = format!("{:?}", guard);
}
