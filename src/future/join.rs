#![allow(non_snake_case)]

use core::mem;

use {Future, Poll, IntoFuture, Async};
use task::Task;

macro_rules! generate {
    ($(
        $(#[$doc:meta])*
        ($Join:ident, $new:ident, <A, $($B:ident),*>),
    )*) => ($(
        $(#[$doc])*
        #[must_use = "futures do nothing unless polled"]
        pub struct $Join<A, $($B),*>
            where A: Future,
                  $($B: Future<Error=A::Error>),*
        {
            a: MaybeDone<A>,
            $($B: MaybeDone<$B>,)*
        }

        pub fn $new<A, $($B),*>(a: A, $($B: $B),*) -> $Join<A, $($B),*>
            where A: Future,
                  $($B: Future<Error=A::Error>),*
        {
            $Join {
                a: MaybeDone::NotYet(a),
                $($B: MaybeDone::NotYet($B)),*
            }
        }

        impl<A, $($B),*> $Join<A, $($B),*>
            where A: Future,
                  $($B: Future<Error=A::Error>),*
        {
            fn erase(&mut self) {
                self.a = MaybeDone::Gone;
                $(self.$B = MaybeDone::Gone;)*
            }
        }

        impl<A, $($B),*> Future for $Join<A, $($B),*>
            where A: Future,
                  $($B: Future<Error=A::Error>),*
        {
            type Item = (A::Item, $($B::Item),*);
            type Error = A::Error;

            fn poll(&mut self, task: &Task) -> Poll<Self::Item, Self::Error> {
                let mut all_done = match self.a.poll(task) {
                    Ok(done) => done,
                    Err(e) => {
                        self.erase();
                        return Err(e)
                    }
                };
                $(
                    all_done = match self.$B.poll(task) {
                        Ok(done) => all_done && done,
                        Err(e) => {
                            self.erase();
                            return Err(e)
                        }
                    };
                )*

                if all_done {
                    Ok(Async::Ready((self.a.take(), $(self.$B.take()),*)))
                } else {
                    Ok(Async::NotReady)
                }
            }
        }

        impl<A, $($B),*> IntoFuture for (A, $($B),*)
            where A: IntoFuture,
        $(
            $B: IntoFuture<Error=A::Error>
        ),*
        {
            type Future = $Join<A::Future, $($B::Future),*>;
            type Item = (A::Item, $($B::Item),*);
            type Error = A::Error;

            fn into_future(self) -> Self::Future {
                match self {
                    (a, $($B),+) => {
                        $new(
                            IntoFuture::into_future(a),
                            $(IntoFuture::into_future($B)),+
                        )
                    }
                }
            }
        }

    )*)
}

generate! {
    /// Future for the `join` combinator, waiting for two futures to
    /// complete.
    ///
    /// This is created by the `Future::join` method.
    (Join, new, <A, B>),

    /// Future for the `join3` combinator, waiting for three futures to
    /// complete.
    ///
    /// This is created by the `Future::join3` method.
    (Join3, new3, <A, B, C>),

    /// Future for the `join4` combinator, waiting for four futures to
    /// complete.
    ///
    /// This is created by the `Future::join4` method.
    (Join4, new4, <A, B, C, D>),

    /// Future for the `join5` combinator, waiting for five futures to
    /// complete.
    ///
    /// This is created by the `Future::join5` method.
    (Join5, new5, <A, B, C, D, E>),
}

enum MaybeDone<A: Future> {
    NotYet(A),
    Done(A::Item),
    Gone,
}

impl<A: Future> MaybeDone<A> {
    fn poll(&mut self, task: &Task) -> Result<bool, A::Error> {
        let res = match *self {
            MaybeDone::NotYet(ref mut a) => try!(a.poll(task)),
            MaybeDone::Done(_) => return Ok(true),
            MaybeDone::Gone => panic!("cannot poll Join twice"),
        };
        match res {
            Async::Ready(res) => {
                *self = MaybeDone::Done(res);
                Ok(true)
            }
            Async::NotReady => Ok(false),
        }
    }

    fn take(&mut self) -> A::Item {
        match mem::replace(self, MaybeDone::Gone) {
            MaybeDone::Done(a) => a,
            _ => panic!(),
        }
    }
}
