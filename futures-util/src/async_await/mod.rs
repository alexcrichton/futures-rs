//! Await
//!
//! This module contains a number of functions and combinators for working
//! with `async`/`await` code.

use core::marker::Unpin;
use futures_core::future::Future;

#[macro_use]
mod poll;
pub use self::poll::*;

#[macro_use]
mod pending;
pub use self::pending::*;

// Primary export is a macro
mod join;

// Primary export is a macro
mod select;

#[doc(hidden)]
#[inline(always)]
pub fn assert_unpin<T: Future + Unpin>(_: &T) {}
