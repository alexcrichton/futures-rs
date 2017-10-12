use std::any::Any;
use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Relaxed};

use {Async, Future, Poll};
use task::{self, Task};

/// A type of futures-powered synchronization primitive which is a mutex between
/// two possible owners.
///
/// This primitive is not as generic as a full-blown mutex but is sufficient for
/// many use cases where there are only two possible owners of a resource. The
/// implementation of `BiLock` can be more optimized for just the two possible
/// owners.
///
/// Note that it's possible to use this lock through a poll-style interface with
/// the `poll_lock` method but you can also use it as a future with the `lock`
/// method that consumes a `BiLock` and returns a future that will resolve when
/// it's locked.
///
/// A `BiLock` is typically used for "split" operations where data which serves
/// two purposes wants to be split into two to be worked with separately. For
/// example a TCP stream could be both a reader and a writer or a framing layer
/// could be both a stream and a sink for messages. A `BiLock` enables splitting
/// these two and then using each independently in a futures-powered fashion.
#[derive(Debug)]
pub struct BiLock<T> {
    inner: Arc<Inner<T>>,
    index: bool
}

#[derive(Debug)]
struct Inner<T> {
    state: AtomicUsize,
    tasks: [UnsafeCell<Option<Task>>; 2],
    value: Option<UnsafeCell<T>>,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> BiLock<T> {
    /// Creates a new `BiLock` protecting the provided data.
    ///
    /// Two handles to the lock are returned, and these are the only two handles
    /// that will ever be available to the lock. These can then be sent to separate
    /// tasks to be managed there.
    pub fn new(t: T) -> (BiLock<T>, BiLock<T>) {
        let inner = Arc::new(Inner {
            state: AtomicUsize::new(3),
            tasks: Default::default(),
            value: Some(UnsafeCell::new(t)),
        });

        (BiLock { inner: inner.clone(), index: false }, BiLock { inner: inner, index: true })
    }

    /// Attempt to acquire this lock, returning `NotReady` if it can't be
    /// acquired.
    ///
    /// This function will acquire the lock in a nonblocking fashion, returning
    /// immediately if the lock is already held. If the lock is successfully
    /// acquired then `Async::Ready` is returned with a value that represents
    /// the locked value (and can be used to access the protected data). The
    /// lock is unlocked when the returned `BiLockGuard` is dropped.
    ///
    /// # Panics
    ///
    /// This function will panic if called outside the context of a future's
    /// task, or if the lock is already held.
    pub fn poll_lock(&mut self) -> Async<BiLockGuard<T>> {
        if unsafe { self.inner.poll_lock(self.index) } {
            Async::Ready(BiLockGuard { inner: self })
        } else {
            Async::NotReady
        }
    }

    /// Perform a "blocking lock" of this lock, consuming this lock handle and
    /// returning a future to the acquired lock.
    ///
    /// This function consumes the `BiLock<T>` and returns a sentinel future,
    /// `BiLockAcquire<T>`. The returned future will resolve to
    /// `BiLockAcquired<T>` which represents a locked lock similarly to
    /// `BiLockGuard<T>`.
    ///
    /// Note that the returned future will never resolve to an error.
    pub fn lock(self) -> BiLockAcquire<T> {
        BiLockAcquire {
            inner: Some(self),
        }
    }

    /// Attempts to put the two "halves" of a `BiLock<T>` back together and
    /// recover the original value. Succeeds only if the two `BiLock<T>`s
    /// originated from the same call to `BiLock::new`.
    pub fn reunite(self, other: Self) -> Result<T, ReuniteError<T>> {
        if &*self.inner as *const _ == &*other.inner as *const _ {
            drop(other);
            let inner = Arc::try_unwrap(self.inner)
                .ok()
                .expect("futures: try_unwrap failed in BiLock<T>::reunite");
            Ok(unsafe { inner.into_inner() })
        } else {
            Err(ReuniteError(self, other))
        }
    }

    fn unlock(&mut self) {
        unsafe {
            self.inner.unlock(self.index);
        }
    }
}

impl<T> Inner<T> {
    unsafe fn into_inner(mut self) -> T {
        self.value.take().unwrap().into_inner()
    }

    unsafe fn poll_lock(&self, caller_index: bool) -> bool {
        let mut state;
        if caller_index {
            // This needs precisely Acquire/Release semantics
            state = self.state.fetch_add(3, AcqRel);

            // Slow path if lock failed
            match state % 6 {
                2 => {
                    *self.tasks.get_unchecked(0).get() = Some(task::current());
                    state = self.state.fetch_add(3, AcqRel);
                },
                5 => {
                    *self.tasks.get_unchecked(1).get() = Some(task::current());
                    state = self.state.fetch_add(3, AcqRel);
                },
                0 | 1 => panic!("Lock already held"),
                _ => {}
            }

            // Make sure the state wraps around at a multiple of 6, and leave plenty of
            // room at the end.
            const UPPER_LIMIT: usize = !0usize - 63;
            if state >= UPPER_LIMIT {
                self.state.fetch_sub(UPPER_LIMIT, Relaxed);
            }
        } else {
            // This needs precisely Acquire/Release semantics
            state = self.state.fetch_xor(1, AcqRel);

            // Slow path if lock failed
            match state % 6 {
                0 => {
                    *self.tasks.get_unchecked(1).get() = Some(task::current());
                    state = self.state.fetch_xor(1, AcqRel);
                },
                1 => {
                    *self.tasks.get_unchecked(0).get() = Some(task::current());
                    state = self.state.fetch_xor(1, AcqRel);
                },
                2 | 5 => panic!("Lock already held"),
                _ => {}
            }
        }

        match state % 6 {
            3 | 4 => true,
            _ => false
        }
    }

    unsafe fn unlock(&self, caller_index: bool) {
        if caller_index {
            // This needs precisely Acquire/Release semantics
            let state = self.state.fetch_add(3, AcqRel);

            // Wake up a waiting task if necessary
            match state % 6 {
                0 => (*self.tasks.get_unchecked(1).get()).take().map(|t| t.notify()),
                1 => (*self.tasks.get_unchecked(0).get()).take().map(|t| t.notify()),
                _ => panic!("Lock not held")
            };
        } else {
            // This needs precisely Acquire/Release semantics
            let state = self.state.fetch_xor(1, AcqRel);

            // Wake up a waiting task if necessary
            match state % 6 {
                2 => (*self.tasks.get_unchecked(0).get()).take().map(|t| t.notify()),
                5 => (*self.tasks.get_unchecked(1).get()).take().map(|t| t.notify()),
                _ => panic!("Lock not held")
            };
        }
    }

    unsafe fn get_value(&self) -> &mut T {
        &mut *self.value.as_ref().unwrap().get()
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        assert!((*self.state.get_mut() + 1) % 6 > 3);
    }
}

/// Error indicating two `BiLock<T>`s were not two halves of a whole, and
/// thus could not be `reunite`d.
pub struct ReuniteError<T>(pub BiLock<T>, pub BiLock<T>);

impl<T> fmt::Debug for ReuniteError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_tuple("ReuniteError")
            .field(&"...")
            .finish()
    }
}

impl<T> fmt::Display for ReuniteError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "tried to reunite two BiLocks that don't form a pair")
    }
}

impl<T: Any> Error for ReuniteError<T> {
    fn description(&self) -> &str {
        "tried to reunite two BiLocks that don't form a pair"
    }
}

/// Returned RAII guard from the `poll_lock` method.
///
/// This structure acts as a sentinel to the data in the `BiLock<T>` itself,
/// implementing `Deref` and `DerefMut` to `T`. When dropped, the lock will be
/// unlocked.
#[derive(Debug)]
pub struct BiLockGuard<'a, T: 'a> {
    inner: &'a mut BiLock<T>,
}

impl<'a, T> Deref for BiLockGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { self.inner.inner.get_value() }
    }
}

impl<'a, T> DerefMut for BiLockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { self.inner.inner.get_value() }
    }
}

impl<'a, T> Drop for BiLockGuard<'a, T> {
    fn drop(&mut self) {
        self.inner.unlock();
    }
}

/// Future returned by `BiLock::lock` which will resolve when the lock is
/// acquired.
#[derive(Debug)]
pub struct BiLockAcquire<T> {
    inner: Option<BiLock<T>>,
}

impl<T> Future for BiLockAcquire<T> {
    type Item = BiLockAcquired<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<BiLockAcquired<T>, ()> {
        match self.inner.as_mut().expect("cannot poll after Ready").poll_lock() {
            Async::Ready(r) => {
                mem::forget(r);
            }
            Async::NotReady => return Ok(Async::NotReady),
        }
        Ok(Async::Ready(BiLockAcquired {
            inner: self.inner.take()
        }))
    }
}

/// Resolved value of the `BiLockAcquire<T>` future.
///
/// This value, like `BiLockGuard<T>`, is a sentinel to the value `T` through
/// implementations of `Deref` and `DerefMut`. When dropped will unlock the
/// lock, and the original unlocked `BiLock<T>` can be recovered through the
/// `unlock` method.
#[derive(Debug)]
pub struct BiLockAcquired<T> {
    inner: Option<BiLock<T>>,
}

impl<T> BiLockAcquired<T> {
    /// Recovers the original `BiLock<T>`, unlocking this lock.
    pub fn unlock(mut self) -> BiLock<T> {
        let mut bi_lock = self.inner.take().unwrap();
        bi_lock.unlock();
        bi_lock
    }
}

impl<T> Deref for BiLockAcquired<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { self.inner.as_ref().unwrap().inner.get_value() }
    }
}

impl<T> DerefMut for BiLockAcquired<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { self.inner.as_ref().unwrap().inner.get_value() }
    }
}

impl<T> Drop for BiLockAcquired<T> {
    fn drop(&mut self) {
        if let Some(ref mut bi_lock) = self.inner {
            bi_lock.unlock();
        }
    }
}
