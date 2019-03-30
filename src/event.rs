//! Event subscriptions.

use std::fmt;
use std::time::Duration;

/// A trait which is provided with information about events in a connection pool.
pub trait HandleEvent: fmt::Debug + Sync + Send {
    /// Called when a new connection is acquired.
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn handle_acquire(&self, event: AcquireEvent) {}

    /// Called when a connection is released.
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn handle_release(&self, event: ReleaseEvent) {}

    /// Called when a connection is checked out from the pool.
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn handle_checkout(&self, event: CheckoutEvent) {}

    /// Called when a checkout attempt times out.
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn handle_timeout(&self, event: TimeoutEvent) {}

    /// Called when a connection is checked back into the pool.
    #[allow(unused_variables)]
    fn handle_checkin(&self, event: CheckinEvent) {}
}

/// A `HandleEvent` implementation which does nothing.
#[derive(Copy, Clone, Debug)]
pub struct NopEventHandler;

impl HandleEvent for NopEventHandler {}

/// Information about an acquire event.
#[derive(Debug)]
pub struct AcquireEvent {
    pub(crate) id: u64,
}

impl AcquireEvent {
    /// Returns the ID of the connection.
    #[inline]
    pub fn connection_id(&self) -> u64 {
        self.id
    }
}

/// Information about a release event.
#[derive(Debug)]
pub struct ReleaseEvent {
    pub(crate) id: u64,
    pub(crate) age: Duration,
}

impl ReleaseEvent {
    /// Returns the ID of the connection.
    #[inline]
    pub fn connection_id(&self) -> u64 {
        self.id
    }

    /// Returns the age of the connection.
    #[inline]
    pub fn age(&self) -> Duration {
        self.age
    }
}

/// Information about a checkout event.
#[derive(Debug)]
pub struct CheckoutEvent {
    pub(crate) id: u64,
    pub(crate) duration: Duration,
}

impl CheckoutEvent {
    /// Returns the ID of the connection.
    #[inline]
    pub fn connection_id(&self) -> u64 {
        self.id
    }

    /// Returns the time taken to check out the connection.
    #[inline]
    pub fn duration(&self) -> Duration {
        self.duration
    }
}

/// Information about a timeout event.
#[derive(Debug)]
pub struct TimeoutEvent {
    pub(crate) timeout: Duration,
}

impl TimeoutEvent {
    /// Returns the timeout of the failed checkout attempt.
    #[inline]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Information about a checkin event.
#[derive(Debug)]
pub struct CheckinEvent {
    pub(crate) id: u64,
    pub(crate) duration: Duration,
}

impl CheckinEvent {
    /// Returns the ID of the connection.
    #[inline]
    pub fn connection_id(&self) -> u64 {
        self.id
    }

    /// Returns the amount of time the connection was checked out.
    #[inline]
    pub fn duration(&self) -> Duration {
        self.duration
    }
}
