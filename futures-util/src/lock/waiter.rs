use crate::lock::backoff::Backoff;
use slab::Slab;
use std::cell::UnsafeCell;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Waker;

#[allow(clippy::identity_op)]
const IS_LOCKED: usize = 1 << 0;
const NOTIFIED: usize = 1 << 1;
const NOTIFIABLE: usize = 1 << 2;

#[derive(Copy, Clone, PartialEq, Eq)]
enum Notify {
    Any,
    All,
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

    fn wake(&mut self) -> bool {
        match mem::replace(self, Waiter::Woken) {
            Waiter::Waiting(waker) => waker.wake(),
            Waiter::Woken => return false,
        };

        true
    }
}

struct Inner {
    waiters: Slab<Waiter>,
    waiting: usize,
}

pub(crate) struct WaiterSet {
    state: AtomicUsize,
    inner: UnsafeCell<Inner>,
}

impl WaiterSet {
    pub(crate) fn new() -> WaiterSet {
        WaiterSet {
            state: AtomicUsize::new(0),
            inner: UnsafeCell::new(Inner {
                waiters: Slab::new(),
                waiting: 0,
            }),
        }
    }

    pub(crate) fn insert(&self, waker: &Waker) -> usize {
        let mut this = self.lock();
        this.waiting += 1;
        this.waiters.insert(Waiter::Waiting(waker.clone()))
    }

    pub(crate) fn register(&self, key: usize, waker: &Waker) {
        let mut this = self.lock();
        this.waiting += 1;
        this.waiters[key].register(waker);
    }

    pub(crate) fn remove(&self, key: usize) {
        let mut this = self.lock();
        match this.waiters.remove(key) {
            Waiter::Waiting(_) => this.waiting -= 1,
            Waiter::Woken => {}
        }
    }

    // NOTE: This method is likely useful for `Mutex`.
    #[allow(dead_code)]
    pub(crate) fn cancel(&self, key: usize) -> bool {
        let mut this = self.lock();
        match this.waiters.remove(key) {
            Waiter::Waiting(_) => this.waiting -= 1,
            Waiter::Woken => {
                for (_, waiter) in this.waiters.iter_mut() {
                    if waiter.wake() {
                        this.waiting -= 1;
                        return true;
                    }
                }
            }
        }

        false
    }

    // NOTE: This method is likely useful for `Mutex`.
    #[allow(dead_code)]
    pub(crate) fn notify_any(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);

        if state & NOTIFIED == 0 && state & NOTIFIABLE != 0 {
            self.notify(Notify::Any)
        } else {
            false
        }
    }

    pub(crate) fn notify_all(&self) -> bool {
        if self.state.load(Ordering::SeqCst) & NOTIFIABLE != 0 {
            self.notify(Notify::All)
        } else {
            false
        }
    }

    fn notify(&self, n: Notify) -> bool {
        let mut this = &mut *self.lock();
        let mut notified = false;

        for (_, waiter) in this.waiters.iter_mut() {
            if waiter.wake() {
                this.waiting -= 1;
                notified = true;
            }

            if n == Notify::Any {
                break;
            }
        }

        notified
    }

    fn lock(&self) -> WaiterSetGuard<'_> {
        let backoff = Backoff::new();
        while self.state.fetch_or(IS_LOCKED, Ordering::Acquire) & IS_LOCKED != 0 {
            backoff.snooze();
        }
        WaiterSetGuard { waiter_set: self }
    }
}

struct WaiterSetGuard<'a> {
    waiter_set: &'a WaiterSet,
}

impl Drop for WaiterSetGuard<'_> {
    fn drop(&mut self) {
        let mut state = 0;

        if self.waiters.len() - self.waiting > 0 {
            state |= NOTIFIED;
        }

        if self.waiting > 0 {
            state |= NOTIFIABLE;
        }

        self.waiter_set.state.store(state, Ordering::SeqCst);
    }
}

impl Deref for WaiterSetGuard<'_> {
    type Target = Inner;

    fn deref(&self) -> &Inner {
        unsafe { &*self.waiter_set.inner.get() }
    }
}

impl DerefMut for WaiterSetGuard<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        unsafe { &mut *self.waiter_set.inner.get() }
    }
}

unsafe impl Send for WaiterSet {}
unsafe impl Sync for WaiterSet {}
