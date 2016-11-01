extern crate futures;

use futures::{task, done, Future, Async};
use futures::stream::mpsc::*;
use std::sync::Arc;
use std::sync::atomic::*;
use futures::stream::Stream;

mod support;
use support::*;

fn send(n: u32, sender: Sender<u32, u32>) -> Box<Future<Item = (), Error = ()> + Send> {
    if n == 0 {
        return done(Ok(())).boxed();
    }
    sender.send(Ok(n))
        .map_err(|_| ())
        .and_then(move |sender| send(n - 1, sender))
        .boxed()
}

#[test]
fn channel_is_not_ready_when_created() {
    let (_tx, mut rx) = channel::<(), ()>();

    sassert_empty(&mut rx);
    sassert_empty(&mut rx);
}

#[test]
fn multiple_senders() {
    let (tx, rx) = channel();
    let tx2 = tx.clone();
    let tx3 = tx.clone();
    let amt = 40;
    send(amt, tx).forget();
    send(amt, tx2).forget();
    send(amt, tx3).forget();
    let mut rx = rx.wait();
    for _ in 1..(amt * 3 + 1) {
        assert!(rx.next().is_some());
    }
    assert_eq!(rx.next(), None);
}

#[test]
fn sequence() {
    let (tx, rx) = channel();

    let amt = 20;
    send(amt, tx).forget();
    let mut rx = rx.wait();
    for i in (1..amt + 1).rev() {
        assert_eq!(rx.next(), Some(Ok(i)));
    }
    assert_eq!(rx.next(), None);
}

#[test]
fn drop_sender() {
    let (tx, mut rx) = channel::<u32, u32>();
    drop(tx);
    sassert_done(&mut rx);
}

#[test]
fn drop_rx() {
    let (tx, rx) = channel::<u32, u32>();
    let tx = tx.send(Ok(1)).wait().ok().unwrap();
    drop(rx);
    assert!(tx.send(Ok(1)).wait().is_err());
}

struct Unpark;

impl task::Unpark for Unpark {
    fn unpark(&self) {}
}

#[test]
fn poll_future_then_drop() {
    let (tx, _rx) = channel::<u32, u32>();

    let tx = tx.send(Ok(1));
    let mut t = task::spawn(tx);

    // First poll succeeds
    let tx = match t.poll_future(Arc::new(Unpark)) {
        Ok(Async::Ready(tx)) => tx,
        _ => panic!(),
    };

    // Send another value
    let tx = tx.send(Ok(2));
    let mut t = task::spawn(tx);

    // Second poll doesn't
    match t.poll_future(Arc::new(Unpark)) {
        Ok(Async::NotReady) => {}
        _ => panic!(),
    };

    drop(t);
}

#[test]
fn drop_order() {
    static DROPS: AtomicUsize = ATOMIC_USIZE_INIT;
    let (tx, rx) = channel::<_, u32>();

    struct A;

    impl Drop for A {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::SeqCst);
        }
    }

    let tx = tx.send(Ok(A)).wait().unwrap();
    assert_eq!(DROPS.load(Ordering::SeqCst), 0);
    drop(rx);
    assert_eq!(DROPS.load(Ordering::SeqCst), 1);
    assert!(tx.send(Ok(A)).wait().is_err());
    assert_eq!(DROPS.load(Ordering::SeqCst), 2);
}
