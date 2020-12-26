use std::cell::Cell;
use std::sync::atomic::spin_loop_hint;
use std::thread::yield_now;

// Taken from crossbeam_utils
const SPIN_LIMIT: u32 = 6;
const YIELD_LIMIT: u32 = 10;

pub(crate) struct Backoff {
    step: Cell<u32>,
}

impl Backoff {
    pub(crate) fn new() -> Backoff {
        Backoff {
            step: Cell::new(0u32),
        }
    }

    pub(crate) fn snooze(&self) {
        if self.step.get() <= SPIN_LIMIT {
            for _ in 0..1 << self.step.get() {
                spin_loop_hint();
            }
        } else {
            yield_now();
        }
        if self.step.get() <= YIELD_LIMIT {
            self.step.set(self.step.get() + 1);
        }
    }
}
