//! Task notification

mod noop_waker;
pub use self::noop_waker::{noop_local_waker, noop_local_waker_ref};

mod spawn;
pub use self::spawn::{SpawnExt, LocalSpawnExt};

#[cfg(feature = "std")]
mod local_waker_ref;
#[cfg(feature = "std")]
pub use self::local_waker_ref::{local_waker_ref, local_waker_ref_from_nonlocal, LocalWakerRef};

#[cfg_attr(
    feature = "nightly",
    cfg(all(target_has_atomic = "cas", target_has_atomic = "ptr"))
)]
pub use futures_core::task::__internal::AtomicWaker;
