#![feature(test)]

extern crate futures;
extern crate futures_executor;
extern crate test;

use futures::prelude::*;
use futures::task::{self, Task};
use futures_executor::current_thread::run;

use test::Bencher;

#[bench]
fn thread_yield_single_thread_one_wait(b: &mut Bencher) {
    const NUM: usize = 10_000;

    struct Yield {
        rem: usize,
    }

    impl Future for Yield {
        type Item = ();
        type Error = ();

        fn poll(&mut self, _ctx: &mut task::Context) -> Poll<(), ()> {
            if self.rem == 0 {
                Ok(Async::Ready(()))
            } else {
                self.rem -= 1;
                task::current().notify();
                Ok(Async::Pending)
            }
        }
    }

    b.iter(|| {
        let y = Yield { rem: NUM };
        run(|c| c.block_on(y).unwrap());
    });
}

#[bench]
fn thread_yield_single_thread_many_wait(b: &mut Bencher) {
    const NUM: usize = 10_000;

    struct Yield {
        rem: usize,
    }

    impl Future for Yield {
        type Item = ();
        type Error = ();

        fn poll(&mut self, _ctx: &mut task::Context) -> Poll<(), ()> {
            if self.rem == 0 {
                Ok(Async::Ready(()))
            } else {
                self.rem -= 1;
                task::current().notify();
                Ok(Async::Pending)
            }
        }
    }

    b.iter(|| {
        for _ in 0..NUM {
            let y = Yield { rem: 1 };
            run(|c| c.block_on(y).unwrap());
        }
    });
}

#[bench]
fn thread_yield_multi_thread(b: &mut Bencher) {
    use std::sync::mpsc;
    use std::thread;

    const NUM: usize = 1_000;

    let (tx, rx) = mpsc::sync_channel::<Task>(10_000);

    struct Yield {
        rem: usize,
        tx: mpsc::SyncSender<Task>,
    }

    impl Future for Yield {
        type Item = ();
        type Error = ();

        fn poll(&mut self, _ctx: &mut task::Context) -> Poll<(), ()> {
            if self.rem == 0 {
                Ok(Async::Ready(()))
            } else {
                self.rem -= 1;
                self.tx.send(task::current()).unwrap();
                Ok(Async::Pending)
            }
        }
    }

    thread::spawn(move || {
        while let Ok(task) = rx.recv() {
            task.notify();
        }
    });

    b.iter(move || {
        let y = Yield {
            rem: NUM,
            tx: tx.clone(),
        };

        run(|c| c.block_on(y).unwrap());
    });
}
