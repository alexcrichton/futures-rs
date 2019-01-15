use Sink;
use executor;

/// A sink combinator which converts an asynchronous sink to a **blocking
/// sink**.
///
/// Created by the `Sink::wait` method, this function transforms any sink into a
/// blocking version. This is implemented by blocking the current thread when a
/// sink is otherwise unable to make progress.
#[must_use = "sinks do nothing unless polled"]
#[derive(Debug)]
pub struct Wait<Si> {
    sink: executor::Spawn<Si>,
}

pub fn new<Si: Sink>(s: Si) -> Wait<Si> {
    Wait {
        sink: executor::spawn(s),
    }
}

impl<Si: Sink> Wait<Si> {
    /// Sends a value to this sink, blocking the current thread until it's able
    /// to do so.
    ///
    /// This function will take the `value` provided and call the underlying
    /// sink's `start_send` function until it's ready to accept the value. If
    /// the function returns `Pending` then the current thread is blocked
    /// until it is otherwise ready to accept the value.
    ///
    /// # Return value
    ///
    /// If `Ok(())` is returned then the `value` provided was successfully sent
    /// along the sink, and if `Err(e)` is returned then an error occurred
    /// which prevented the value from being sent.
    pub fn send(&mut self, value: Si::SinkItem) -> Result<(), Si::SinkError> {
        self.sink.wait_send(value)
    }

    /// Flushes any buffered data in this sink, blocking the current thread
    /// until it's entirely flushed.
    ///
    /// This function will call the underlying sink's `flush` method
    /// until it returns that it's ready to proceed. If the method returns
    /// `Pending` the current thread will be blocked until it's otherwise
    /// ready to proceed.
    pub fn flush(&mut self) -> Result<(), Si::SinkError> {
        self.sink.wait_flush()
    }

    /// Close this sink, blocking the current thread until it's entirely closed.
    ///
    /// This function will call the underlying sink's `close` method
    /// until it returns that it's closed. If the method returns
    /// `Pending` the current thread will be blocked until it's otherwise
    /// closed.
    pub fn close(&mut self) -> Result<(), Si::SinkError> {
        self.sink.wait_close()
    }
}
