#![feature(test)]

extern crate futures;
extern crate test;

use futures::prelude::*;
use futures::task::{self, Task, Notify, NotifyHandle};

use test::Bencher;

fn notify_noop() -> NotifyHandle {
    struct Noop;

    impl Notify for Noop {
        fn notify(&self, _id: usize) {}
    }

    const NOOP : &'static Noop = &Noop;

    NotifyHandle::from(NOOP)
}

#[bench]
fn task_init(b: &mut Bencher) {
    const NUM: u32 = 100_000;

    struct MyFuture {
        num: u32,
        task: Option<Task>,
    };

    impl Future for MyFuture {
        type Item = ();
        type Error = ();

        fn poll(&mut self, _ctx: &mut task::Context) -> Poll<(), ()> {
            if self.num == NUM {
                Ok(Async::Ready(()))
            } else {
                self.num += 1;

                if let Some(ref t) = self.task {
                    if t.will_notify_current() {
                        t.notify();
                        return Ok(Async::Pending);
                    }
                }

                let t = task::current();
                t.notify();
                self.task = Some(t);

                Ok(Async::Pending)
            }
        }
    }

    let notify = notify_noop();

    let mut fut = task::spawn(MyFuture {
        num: 0,
        task: None,
    });

    b.iter(|| {
        fut.get_mut().num = 0;

        while let Ok(Async::Pending) = fut.poll_future_notify(&notify, 0) {
        }
    });
}
