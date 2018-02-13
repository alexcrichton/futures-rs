use std::prelude::v1::*;

use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::fmt;

use futures::channel::oneshot::{channel, Sender, Receiver};
use futures::future::lazy;
use futures::prelude::*;
use futures::task::{self, Notify, Spawn};
use futures_executor::enter;
use num_cpus;

use unpark_mutex::UnparkMutex;

/// A thread pool intended to run CPU intensive work.
///
/// This thread pool will hand out futures representing the completed work
/// that happens on the thread pool itself, and the futures can then be later
/// composed with other work as part of an overall computation.
///
/// The worker threads associated with a thread pool are kept alive so long as
/// there is an open handle to the `CpuPool` or there is work running on them. Once
/// all work has been drained and all references have gone away the worker
/// threads will be shut down.
///
/// Currently `CpuPool` implements `Clone` which just clones a new reference to
/// the underlying thread pool.
///
/// **Note:** if you use CpuPool inside a library it's better accept a
/// `Builder` object for thread configuration rather than configuring just
/// pool size.  This not only future proof for other settings but also allows
/// user to attach monitoring tools to lifecycle hooks.
pub struct CpuPool {
    inner: Arc<Inner>,
}

/// Thread pool configuration object
///
/// Builder starts with a number of workers equal to the number
/// of CPUs on the host. But you can change it until you call `create()`.
pub struct Builder {
    pool_size: usize,
    stack_size: usize,
    name_prefix: Option<String>,
    after_start: Option<Arc<Fn(usize) + Send + Sync>>,
    before_stop: Option<Arc<Fn(usize) + Send + Sync>>,
}

struct MySender<F, T> {
    fut: F,
    tx: Option<Sender<T>>,
    keep_running_flag: Arc<AtomicBool>,
}

trait AssertSendSync: Send + Sync {}
impl AssertSendSync for CpuPool {}

struct Inner {
    tx: Mutex<mpsc::Sender<Message>>,
    rx: Mutex<mpsc::Receiver<Message>>,
    cnt: AtomicUsize,
    size: usize,
}

impl fmt::Debug for CpuPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("CpuPool")
            .field("size", &self.inner.size)
            .finish()
    }
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Builder")
            .field("pool_size", &self.pool_size)
            .field("name_prefix", &self.name_prefix)
            .finish()
    }
}

/// The type of future returned from the `CpuPool::spawn` function, which
/// proxies the futures running on the thread pool.
///
/// This future will resolve in the same way as the underlying future, and it
/// will propagate panics.
#[must_use]
#[derive(Debug)]
pub struct CpuFuture<T, E> {
    inner: Receiver<thread::Result<Result<T, E>>>,
    keep_running_flag: Arc<AtomicBool>,
}

enum Message {
    Run(Run),
    Close,
}

impl CpuPool {
    /// Creates a new thread pool with `size` worker threads associated with it.
    ///
    /// The returned handle can use `execute` to run work on this thread pool,
    /// and clones can be made of it to get multiple references to the same
    /// thread pool.
    ///
    /// This is a shortcut for:
    ///
    /// ```rust
    /// # use futures_cpupool::{Builder, CpuPool};
    /// #
    /// # fn new(size: usize) -> CpuPool {
    /// Builder::new().pool_size(size).create()
    /// # }
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `size == 0`.
    pub fn new(size: usize) -> CpuPool {
        Builder::new().pool_size(size).create()
    }

    /// Creates a new thread pool with a number of workers equal to the number
    /// of CPUs on the host.
    ///
    /// This is a shortcut for:
    ///
    /// ```rust
    /// # use futures_cpupool::{Builder, CpuPool};
    /// #
    /// # fn new_num_cpus() -> CpuPool {
    /// Builder::new().create()
    /// # }
    /// ```
    pub fn new_num_cpus() -> CpuPool {
        Builder::new().create()
    }

    /// TODO
    pub fn execute<F>(&self, future: F)
        where F: Future<Item = (), Error = ()> + Send + 'static
    {
        let run = Run {
            spawn: task::spawn(Box::new(future)),
            inner: Arc::new(RunInner {
                exec: self.inner.clone(),
                mutex: UnparkMutex::new(),
            }),
        };
        self.inner.send(Message::Run(run));
    }

    /// Spawns a future to run on this thread pool, returning a future
    /// representing the produced value.
    ///
    /// This function will execute the future `f` on the associated thread
    /// pool, and return a future representing the finished computation. The
    /// returned future serves as a proxy to the computation that `F` is
    /// running.
    ///
    /// To simply run an arbitrary closure on a thread pool and extract the
    /// result, you can use the `future::lazy` combinator to defer work to
    /// executing on the thread pool itself.
    ///
    /// Note that if the future `f` panics it will be caught by default and the
    /// returned future will propagate the panic. That is, panics will not tear
    /// down the thread pool and will be propagated to the returned future's
    /// `poll` method if queried.
    ///
    /// If the returned future is dropped then this `CpuPool` will attempt to
    /// cancel the computation, if possible. That is, if the computation is in
    /// the middle of working, it will be interrupted when possible.
    pub fn spawn<F>(&self, f: F) -> CpuFuture<F::Item, F::Error>
        where F: Future + Send + 'static,
              F::Item: Send + 'static,
              F::Error: Send + 'static,
    {
        let (tx, rx) = channel();
        let keep_running_flag = Arc::new(AtomicBool::new(false));
        // AssertUnwindSafe is used here because `Send + 'static` is basically
        // an alias for an implementation of the `UnwindSafe` trait but we can't
        // express that in the standard library right now.
        let sender = MySender {
            fut: AssertUnwindSafe(f).catch_unwind(),
            tx: Some(tx),
            keep_running_flag: keep_running_flag.clone(),
        };
        self.execute(sender);
        CpuFuture { inner: rx , keep_running_flag: keep_running_flag.clone() }
    }

    /// Spawns a closure on this thread pool.
    ///
    /// This function is a convenience wrapper around the `spawn` function above
    /// for running a closure wrapped in `future::lazy`. It will spawn the
    /// function `f` provided onto the thread pool, and continue to run the
    /// future returned by `f` on the thread pool as well.
    ///
    /// The returned future will be a handle to the result produced by the
    /// future that `f` returns.
    pub fn spawn_fn<F, R>(&self, f: F) -> CpuFuture<R::Item, R::Error>
        where F: FnOnce() -> R + Send + 'static,
              R: IntoFuture + 'static,
              R::Future: Send + 'static,
              R::Item: Send + 'static,
              R::Error: Send + 'static,
    {
        self.spawn(lazy(f))
    }
}

impl Inner {
    fn send(&self, msg: Message) {
        self.tx.lock().unwrap().send(msg).unwrap();
    }

    fn work(&self,
            idx: usize,
            after_start: Option<Arc<Fn(usize) + Send + Sync>>,
            before_stop: Option<Arc<Fn(usize) + Send + Sync>>) {
        let _scope = enter().unwrap();
        after_start.map(|fun| fun(idx));
        loop {
            let msg = self.rx.lock().unwrap().recv().unwrap();
            match msg {
                Message::Run(r) => r.run(),
                Message::Close => break,
            }
        }
        before_stop.map(|fun| fun(idx));
    }
}

impl Clone for CpuPool {
    fn clone(&self) -> CpuPool {
        self.inner.cnt.fetch_add(1, Ordering::Relaxed);
        CpuPool { inner: self.inner.clone() }
    }
}

impl Drop for CpuPool {
    fn drop(&mut self) {
        if self.inner.cnt.fetch_sub(1, Ordering::Relaxed) == 1 {
            for _ in 0..self.inner.size {
                self.inner.send(Message::Close);
            }
        }
    }
}

impl<T, E> CpuFuture<T, E> {
    /// Drop this future without canceling the underlying future.
    ///
    /// When `CpuFuture` is dropped, `CpuPool` will try to abort the underlying
    /// future. This function can be used when user wants to drop but keep
    /// executing the underlying future.
    pub fn forget(self) {
        self.keep_running_flag.store(true, Ordering::SeqCst);
    }
}

impl<T: Send + 'static, E: Send + 'static> Future for CpuFuture<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self, ctx: &mut task::Context) -> Poll<T, E> {
        match self.inner.poll(ctx).expect("cannot poll CpuFuture twice") {
            Async::Ready(Ok(Ok(e))) => Ok(e.into()),
            Async::Ready(Ok(Err(e))) => Err(e),
            Async::Ready(Err(e)) => panic::resume_unwind(e),
            Async::Pending => Ok(Async::Pending),
        }
    }
}

impl<F: Future> Future for MySender<F, Result<F::Item, F::Error>> {
    type Item = ();
    type Error = ();

    fn poll(&mut self, ctx: &mut task::Context) -> Poll<(), ()> {
        if let Ok(Async::Ready(_)) = self.tx.as_mut().unwrap().poll_cancel(ctx) {
            if !self.keep_running_flag.load(Ordering::SeqCst) {
                // Cancelled, bail out
                return Ok(().into())
            }
        }

        let res = match self.fut.poll(ctx) {
            Ok(Async::Ready(e)) => Ok(e),
            Ok(Async::Pending) => return Ok(Async::Pending),
            Err(e) => Err(e),
        };

        // if the receiving end has gone away then that's ok, we just ignore the
        // send error here.
        drop(self.tx.take().unwrap().send(res));
        Ok(Async::Ready(()))
    }
}

impl Builder {
    /// Create a builder a number of workers equal to the number
    /// of CPUs on the host.
    pub fn new() -> Builder {
        Builder {
            pool_size: num_cpus::get(),
            stack_size: 0,
            name_prefix: None,
            after_start: None,
            before_stop: None,
        }
    }

    /// Set size of a future CpuPool
    ///
    /// The size of a thread pool is the number of worker threads spawned
    pub fn pool_size(&mut self, size: usize) -> &mut Self {
        self.pool_size = size;
        self
    }

    /// Set stack size of threads in the pool.
    pub fn stack_size(&mut self, stack_size: usize) -> &mut Self {
        self.stack_size = stack_size;
        self
    }

    /// Set thread name prefix of a future CpuPool
    ///
    /// Thread name prefix is used for generating thread names. For example, if prefix is
    /// `my-pool-`, then threads in the pool will get names like `my-pool-1` etc.
    pub fn name_prefix<S: Into<String>>(&mut self, name_prefix: S) -> &mut Self {
        self.name_prefix = Some(name_prefix.into());
        self
    }

    /// Execute function `f` right after each thread is started but before
    /// running any jobs on it.
    ///
    /// This is initially intended for bookkeeping and monitoring uses.
    /// The `f` will be deconstructed after the `builder` is deconstructed
    /// and all threads in the pool has executed it.
    ///
    /// The closure provided will receive an index corresponding to which worker
    /// thread it's running on.
    pub fn after_start<F>(&mut self, f: F) -> &mut Self
        where F: Fn(usize) + Send + Sync + 'static
    {
        self.after_start = Some(Arc::new(f));
        self
    }

    /// Execute function `f` before each worker thread stops.
    ///
    /// This is initially intended for bookkeeping and monitoring uses.
    /// The `f` will be deconstructed after the `builder` is deconstructed
    /// and all threads in the pool has executed it.
    ///
    /// The closure provided will receive an index corresponding to which worker
    /// thread it's running on.
    pub fn before_stop<F>(&mut self, f: F) -> &mut Self
        where F: Fn(usize) + Send + Sync + 'static
    {
        self.before_stop = Some(Arc::new(f));
        self
    }

    /// Create CpuPool with configured parameters
    ///
    /// # Panics
    ///
    /// Panics if `pool_size == 0`.
    pub fn create(&mut self) -> CpuPool {
        let (tx, rx) = mpsc::channel();
        let pool = CpuPool {
            inner: Arc::new(Inner {
                tx: Mutex::new(tx),
                rx: Mutex::new(rx),
                cnt: AtomicUsize::new(1),
                size: self.pool_size,
            }),
        };
        assert!(self.pool_size > 0);

        for counter in 0..self.pool_size {
            let inner = pool.inner.clone();
            let after_start = self.after_start.clone();
            let before_stop = self.before_stop.clone();
            let mut thread_builder = thread::Builder::new();
            if let Some(ref name_prefix) = self.name_prefix {
                thread_builder = thread_builder.name(format!("{}{}", name_prefix, counter));
            }
            if self.stack_size > 0 {
                thread_builder = thread_builder.stack_size(self.stack_size);
            }
            thread_builder.spawn(move || inner.work(counter, after_start, before_stop)).unwrap();
        }
        return pool
    }
}

/// Units of work submitted to an `Executor`, currently only created
/// internally.
struct Run {
    spawn: Spawn<Box<Future<Item = (), Error = ()> + Send>>,
    inner: Arc<RunInner>,
}

struct RunInner {
    mutex: UnparkMutex<Run>,
    exec: Arc<Inner>,
}

impl Run {
    /// Actually run the task (invoking `poll` on its future) on the current
    /// thread.
    pub fn run(self) {
        let Run { mut spawn, inner } = self;

        // SAFETY: the ownership of this `Run` object is evidence that
        // we are in the `POLLING`/`REPOLL` state for the mutex.
        unsafe {
            inner.mutex.start_poll();

            loop {
                match spawn.poll_future_notify(&inner, 0) {
                    Ok(Async::Pending) => {}
                    Ok(Async::Ready(())) |
                    Err(()) => return inner.mutex.complete(),
                }
                let run = Run { spawn: spawn, inner: inner.clone() };
                match inner.mutex.wait(run) {
                    Ok(()) => return,            // we've waited
                    Err(r) => spawn = r.spawn,   // someone's notified us
                }
            }
        }
    }
}

impl fmt::Debug for Run {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Run")
         .field("contents", &"...")
         .finish()
    }
}

impl Notify for RunInner {
    fn notify(&self, _id: usize) {
        match self.mutex.notify() {
            Ok(run) => self.exec.send(Message::Run(run)),
            Err(()) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn test_drop_after_start() {
        let (tx, rx) = mpsc::sync_channel(2);
        let _cpu_pool = Builder::new()
            .pool_size(2)
            .after_start(move |_| tx.send(1).unwrap()).create();

        // After Builder is deconstructed, the tx should be droped
        // so that we can use rx as an iterator.
        let count = rx.into_iter().count();
        assert_eq!(count, 2);
    }
}

