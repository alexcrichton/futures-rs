#![feature(test, pin, arbitrary_self_types, futures_api)]

extern crate futures;
extern crate test;

use futures::prelude::*;
use futures::task::{self, Waker, LocalWaker, Wake, local_waker_from_nonlocal};
use futures::executor::LocalPool;

use std::marker::Unpin;
use std::mem::PinMut;
use std::sync::Arc;
use test::Bencher;

fn notify_noop() -> LocalWaker {
    struct Noop;

    impl Wake for Noop {
        fn wake(_: &Arc<Self>) {}
    }

    local_waker_from_nonlocal(Arc::new(Noop))
}

#[bench]
fn task_init(b: &mut Bencher) {
    const NUM: u32 = 100_000;

    struct MyFuture {
        num: u32,
        task: Option<Waker>,
    };
    impl Unpin for MyFuture {}

    impl Future for MyFuture {
        type Output = ();

        fn poll(mut self: PinMut<Self>, cx: &mut task::Context) -> Poll<Self::Output> {
            if self.num == NUM {
                Poll::Ready(())
            } else {
                self.num += 1;

                if let Some(ref t) = self.task {
                    t.wake();
                    return Poll::Pending;
                }

                let t = cx.waker().clone();
                t.wake();
                self.task = Some(t);

                Poll::Pending
            }
        }
    }

    let mut fut = MyFuture {
        num: 0,
        task: None,
    };

    let pool = LocalPool::new();
    let mut exec = pool.executor();
    let waker = notify_noop();
    let mut cx = task::Context::new(&waker, &mut exec);

    b.iter(|| {
        fut.num = 0;
        while let Poll::Pending = fut.poll_unpin(&mut cx) {}
    });
}
