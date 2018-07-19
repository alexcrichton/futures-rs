use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::ptr::{self, NonNull};
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::SeqCst;

use futures_core::task::{UnsafeWake, Waker, LocalWaker};

use super::ReadyToRunQueue;
use super::abort::abort;

pub(super) struct Node<Fut> {
    // The future
    pub(super) future: UnsafeCell<Option<Fut>>,

    // Next pointer for linked list tracking all active nodes
    pub(super) next_all: UnsafeCell<*const Node<Fut>>,

    // Previous node in linked list tracking all active nodes
    pub(super) prev_all: UnsafeCell<*const Node<Fut>>,

    // Next pointer in readiness queue
    pub(super) next_ready_to_run: AtomicPtr<Node<Fut>>,

    // Queue that we'll be enqueued to when notified
    pub(super) ready_to_run_queue: Weak<ReadyToRunQueue<Fut>>,

    // Whether or not this node is currently in the ready to run queue.
    pub(super) queued: AtomicBool,
}

impl<Fut> Node<Fut> {
    pub(super) fn wake(self: &Arc<Node<Fut>>) {
        let inner = match self.ready_to_run_queue.upgrade() {
            Some(inner) => inner,
            None => return,
        };

        // It's our job to notify the node that it's ready to get polled,
        // meaning that we need to enqueue it into the readiness queue. To
        // do this we flag that we're ready to be queued, and if successful
        // we then do the literal queueing operation, ensuring that we're
        // only queued once.
        //
        // Once the node is inserted we be sure to notify the parent task,
        // as it'll want to come along and pick up our node now.
        //
        // Note that we don't change the reference count of the node here,
        // we're just enqueueing the raw pointer. The `FuturesUnordered`
        // implementation guarantees that if we set the `queued` flag true that
        // there's a reference count held by the main `FuturesUnordered` queue
        // still.
        let prev = self.queued.swap(true, SeqCst);
        if !prev {
            inner.enqueue(&**self);
            inner.parent.wake();
        }
    }

    // Clones the `Arc` and casts it to `dyn UnsafeWake`. This method is safe,
    // but the returned `NonNull<dyn UnsafeWake>` must be placed in a waker,
    // otherwise there will be a memory leak.
    fn clone_as_unsafe_wake_without_lifetime(
        self: &Arc<Node<Fut>>,
    ) -> NonNull<dyn UnsafeWake> {
        let clone = self.clone();

        // Safety: This is save because an `Arc` is a struct which contains
        // a single field that is a pointer.
        let ptr = unsafe {
            mem::transmute::<Arc<Node<Fut>>,
                             NonNull<ArcNode<Fut>>>(clone)
        };
        let ptr = ptr as NonNull<dyn UnsafeWake>;

        // Hide lifetime of `Fut`
        // Safety: This is safe because `UnsafeWake` is guaranteed to not
        // touch `Fut`
        unsafe {
            mem::transmute::<NonNull<dyn UnsafeWake>,
                             NonNull<dyn UnsafeWake>>(ptr)
        }
    }

    pub(super) fn local_waker(self: &Arc<Node<Fut>>) -> LocalWaker {
        unsafe { LocalWaker::new(self.clone_as_unsafe_wake_without_lifetime()) }
    }

    pub(super) fn waker(self: &Arc<Node<Fut>>) -> Waker {
        unsafe { Waker::new(self.clone_as_unsafe_wake_without_lifetime()) }
    }
}

impl<Fut> Drop for Node<Fut> {
    fn drop(&mut self) {
        // Currently a `Node<Fut>` is sent across all threads for any lifetime,
        // regardless of `Fut`. This means that for memory safety we can't
        // actually touch `Fut` at any time except when we have a reference to the
        // `FuturesUnordered` itself.
        //
        // Consequently it *should* be the case that we always drop futures from
        // the `FuturesUnordered` instance, but this is a bomb in place to catch
        // any bugs in that logic.
        unsafe {
            if (*self.future.get()).is_some() {
                abort("future still here when dropping");
            }
        }
    }
}

// `ArcNode<Fut>` represents conceptually the struct an `Arc<Node<Fut>>` points
// to. `*const ArcNode<Fut>` is equal to `Arc<Node<Fut>>`
// It may only be used through references because its layout obviously doesn't
// match the real inner struct of an `Arc` which (currently) has the form
// `{ strong, weak, data }`.
struct ArcNode<Fut>(PhantomData<Fut>);

// We should never touch the future `Fut` on any thread other than the one owning
// `FuturesUnordered`, so this should be a safe operation.
unsafe impl<Fut> Send for ArcNode<Fut> {}
unsafe impl<Fut> Sync for ArcNode<Fut> {}

// We need to implement `UnsafeWake` trait directly and can't implement `Wake`
// for `Node<Fut>` because `Fut`, the future, isn't required to have a static
// lifetime. `UnsafeWake` lets us forget about `Fut` and its lifetime. This is
// safe because neither `drop_raw` nor `wake` touch `Fut`. This is the case even
// though `drop_raw` runs the destructor for `Node<Fut>` because its destructor
// is guaranteed to not touch `Fut`. `Fut` must already have been dropped by the
// time it runs. See `Drop` impl for `Node<T>` for more details.
unsafe impl<Fut> UnsafeWake for ArcNode<Fut> {
    #[inline]
    unsafe fn clone_raw(&self) -> Waker {
        let me: *const ArcNode<Fut> = self;
        let node = &*(&me as *const *const ArcNode<Fut>
                          as *const Arc<Node<Fut>>);
        Node::waker(node)
    }

    #[inline]
    unsafe fn drop_raw(&self) {
        let mut me: *const ArcNode<Fut> = self;
        let node_ptr = &mut me as *mut *const ArcNode<Fut>
                               as *mut Arc<Node<Fut>>;
        ptr::drop_in_place(node_ptr);
    }

    #[inline]
    unsafe fn wake(&self) {
        let me: *const ArcNode<Fut> = self;
        let node = &*(&me as *const *const ArcNode<Fut>
                          as *const Arc<Node<Fut>>);
        Node::wake(node)
    }
}
