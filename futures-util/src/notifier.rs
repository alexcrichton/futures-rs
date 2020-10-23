//! Notifier used for waking by `Shared` for both streams and futures.
use crate::task::{ArcWake, Waker};
use futures_core::task::Context;
use slab::Slab;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, SeqCst};
use std::sync::{Arc, Mutex};

const IDLE: usize = 0;
const POLLING: usize = 1;
const COMPLETE: usize = 2;
const POISONED: usize = 3;

pub(crate) const NULL_WAKER_KEY: usize = usize::max_value();

/// An object that contains a polling state, as well as
/// a list of wakers to wake when the state changes.
pub(crate) struct Notifier {
    pub(crate) state: AtomicUsize,
    wakers: Mutex<Option<Slab<Option<Waker>>>>,
}

pub(crate) enum State {
    Idle,
    Polling,
    Complete,
}

pub(crate) struct Reset<'a>(&'a AtomicUsize);

impl Notifier {
    pub(crate) fn new() -> Notifier {
        Notifier {
            state: AtomicUsize::new(IDLE),
            wakers: Mutex::new(Some(Slab::new())),
        }
    }

    pub(crate) fn remove(&self, waker_key: usize) {
        if waker_key != NULL_WAKER_KEY {
            if let Ok(mut wakers) = self.wakers.lock() {
                if let Some(wakers) = wakers.as_mut() {
                    wakers.remove(waker_key);
                }
            }
        }
    }

    pub(crate) fn record_waker(&self, waker_key: &mut usize, cx: &mut Context<'_>) {
        // Acquire the lock first before checking COMPLETE to ensure there
        // isn't a race.
        let mut wakers_guard = self.wakers.lock().unwrap();

        let wakers = match wakers_guard.as_mut() {
            Some(wakers) => wakers,
            None => return, // The value is already available, so there's no need to set the waker.
        };

        let new_waker = cx.waker();

        if *waker_key == NULL_WAKER_KEY {
            *waker_key = wakers.insert(Some(new_waker.clone()));
        } else {
            match wakers[*waker_key] {
                Some(ref old_waker) if new_waker.will_wake(old_waker) => {}
                ref mut slot => *slot = Some(new_waker.clone()),
            }
        }
        debug_assert!(*waker_key != NULL_WAKER_KEY);
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.state.load(Acquire) == COMPLETE
    }

    pub(crate) fn try_start_poll(&self) -> State {
        match self.state.compare_and_swap(IDLE, POLLING, SeqCst) {
            IDLE => State::Idle,
            POLLING => State::Polling,
            COMPLETE => State::Complete,
            POISONED => panic!("panicked during poll"),
            _ => unreachable!(),
        }
    }

    pub(crate) fn set_idle(&self) {
        match self.state.compare_and_swap(POLLING, IDLE, SeqCst) {
            POLLING => {}
            _ => unreachable!(),
        }
    }

    pub(crate) fn finish_poll(&self, _reset: Reset<'_>) {
        self.state.store(COMPLETE, SeqCst);

        // Wake all tasks and drop the slab
        let mut wakers_guard = self.wakers.lock().unwrap();
        let mut wakers = wakers_guard.take().unwrap();
        for opt_waker in wakers.drain() {
            if let Some(waker) = opt_waker {
                waker.wake();
            }
        }
    }

    pub(crate) fn reset_poll(&self, _reset: Reset<'_>) {
        let mut wakers = self.wakers.lock().unwrap();
        self.state.store(IDLE, SeqCst);
        wake_all(&mut wakers.as_mut().unwrap());
    }

    pub(crate) fn reset_guard(&self) -> Reset<'_> {
        Reset(&self.state)
    }
}

impl ArcWake for Notifier {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let wakers = &mut *arc_self.wakers.lock().unwrap();
        if let Some(wakers) = wakers.as_mut() {
            wake_all(wakers)
        }
    }
}

impl Drop for Reset<'_> {
    fn drop(&mut self) {
        use std::thread;

        if thread::panicking() {
            self.0.store(POISONED, SeqCst);
        }
    }
}

fn wake_all(wakers: &mut Slab<Option<Waker>>) {
    for (_, opt_waker) in wakers {
        if let Some(waker) = opt_waker.take() {
            waker.wake()
        }
    }
}
