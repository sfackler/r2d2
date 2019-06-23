//! A generic connection pool.
//!
//! Opening a new database connection every time one is needed is both
//! inefficient and can lead to resource exhaustion under high traffic
//! conditions. A connection pool maintains a set of open connections to a
//! database, handing them out for repeated use.
//!
//! r2d2 is agnostic to the connection type it is managing. Implementors of the
//! `ManageConnection` trait provide the database-specific logic to create and
//! check the health of connections.
//!
//! # Example
//!
//! Using an imaginary "foodb" database.
//!
//! ```rust,ignore
//! use std::thread;
//!
//! extern crate r2d2;
//! extern crate r2d2_foodb;
//!
//! fn main() {
//!     let manager = r2d2_foodb::FooConnectionManager::new("localhost:1234");
//!     let pool = r2d2::Pool::builder()
//!         .max_size(15)
//!         .build(manager)
//!         .unwrap();
//!
//!     for _ in 0..20 {
//!         let pool = pool.clone();
//!         thread::spawn(move || {
//!             let conn = pool.get().unwrap();
//!             // use the connection
//!             // it will be returned to the pool when it falls out of scope.
//!         })
//!     }
//! }
//! ```
#![warn(missing_docs)]
#![doc(html_root_url = "https://docs.rs/r2d2/0.8")]

use log::error;

use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cmp;
use std::error;
use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

pub use crate::config::Builder;
use crate::config::Config;
use crate::event::{AcquireEvent, CheckinEvent, CheckoutEvent, ReleaseEvent, TimeoutEvent};
pub use crate::event::{HandleEvent, NopEventHandler};
pub use crate::extensions::Extensions;

mod config;
pub mod event;
mod extensions;

#[cfg(test)]
mod test;

static CONNECTION_ID: AtomicUsize = AtomicUsize::new(0);

/// A trait which provides connection-specific functionality.
pub trait ManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;

    /// The error type returned by `Connection`s.
    type Error: error::Error + 'static;

    /// Attempts to create a new connection.
    fn connect(&self) -> Result<Self::Connection, Self::Error>;

    /// Determines if the connection is still connected to the database.
    ///
    /// A standard implementation would check if a simple query like `SELECT 1`
    /// succeeds.
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error>;

    /// *Quickly* determines if the connection is no longer usable.
    ///
    /// This will be called synchronously every time a connection is returned
    /// to the pool, so it should *not* block. If it returns `true`, the
    /// connection will be discarded.
    ///
    /// For example, an implementation might check if the underlying TCP socket
    /// has disconnected. Implementations that do not support this kind of
    /// fast health check may simply return `false`.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;
}

/// A trait which handles errors reported by the `ManageConnection`.
pub trait HandleError<E>: fmt::Debug + Send + Sync + 'static {
    /// Handles an error.
    fn handle_error(&self, error: E);
}

/// A `HandleError` implementation which does nothing.
#[derive(Copy, Clone, Debug)]
pub struct NopErrorHandler;

impl<E> HandleError<E> for NopErrorHandler {
    fn handle_error(&self, _: E) {}
}

/// A `HandleError` implementation which logs at the error level.
#[derive(Copy, Clone, Debug)]
pub struct LoggingErrorHandler;

impl<E> HandleError<E> for LoggingErrorHandler
where
    E: error::Error,
{
    fn handle_error(&self, error: E) {
        error!("{}", error);
    }
}

/// A trait which allows for customization of connections.
pub trait CustomizeConnection<C, E>: fmt::Debug + Send + Sync + 'static {
    /// Called with connections immediately after they are returned from
    /// `ManageConnection::connect`.
    ///
    /// The default implementation simply returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// If this method returns an error, the connection will be discarded.
    #[allow(unused_variables)]
    fn on_acquire(&self, conn: &mut C) -> Result<(), E> {
        Ok(())
    }

    /// Called with connections when they are removed from the pool.
    ///
    /// The connections may be broken (as reported by `is_valid` or
    /// `has_broken`), or have simply timed out.
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn on_release(&self, conn: C) {}
}

/// A `CustomizeConnection` which does nothing.
#[derive(Copy, Clone, Debug)]
pub struct NopConnectionCustomizer;

impl<C, E> CustomizeConnection<C, E> for NopConnectionCustomizer {}

struct Conn<C> {
    conn: C,
    extensions: Extensions,
    birth: Instant,
    id: u64,
}

struct IdleConn<C> {
    conn: Conn<C>,
    idle_start: Instant,
}

struct PoolInternals<C> {
    conns: Vec<IdleConn<C>>,
    num_conns: u32,
    pending_conns: u32,
    last_error: Option<String>,
}

struct SharedPool<M>
where
    M: ManageConnection,
{
    config: Config<M::Connection, M::Error>,
    manager: M,
    internals: Mutex<PoolInternals<M::Connection>>,
    cond: Condvar,
}

fn drop_conns<M>(
    shared: &Arc<SharedPool<M>>,
    mut internals: MutexGuard<PoolInternals<M::Connection>>,
    conns: Vec<Conn<M::Connection>>,
) where
    M: ManageConnection,
{
    internals.num_conns -= conns.len() as u32;
    establish_idle_connections(shared, &mut internals);
    drop(internals); // make sure we run connection destructors without this locked

    for conn in conns {
        let event = ReleaseEvent {
            id: conn.id,
            age: conn.birth.elapsed(),
        };
        shared.config.event_handler.handle_release(event);
        shared.config.connection_customizer.on_release(conn.conn);
    }
}

fn establish_idle_connections<M>(
    shared: &Arc<SharedPool<M>>,
    internals: &mut PoolInternals<M::Connection>,
) where
    M: ManageConnection,
{
    let min = shared.config.min_idle.unwrap_or(shared.config.max_size);
    let idle = internals.conns.len() as u32;
    for _ in idle..min {
        add_connection(shared, internals);
    }
}

fn add_connection<M>(shared: &Arc<SharedPool<M>>, internals: &mut PoolInternals<M::Connection>)
where
    M: ManageConnection,
{
    if internals.num_conns + internals.pending_conns >= shared.config.max_size {
        return;
    }

    internals.pending_conns += 1;
    inner(Duration::from_secs(0), shared);

    fn inner<M>(delay: Duration, shared: &Arc<SharedPool<M>>)
    where
        M: ManageConnection,
    {
        let new_shared = Arc::downgrade(shared);
        shared.config.thread_pool.execute_after(delay, move || {
            let shared = match new_shared.upgrade() {
                Some(shared) => shared,
                None => return,
            };

            let conn = shared.manager.connect().and_then(|mut conn| {
                shared
                    .config
                    .connection_customizer
                    .on_acquire(&mut conn)
                    .map(|_| conn)
            });
            match conn {
                Ok(conn) => {
                    let id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed) as u64;

                    let event = AcquireEvent { id };
                    shared.config.event_handler.handle_acquire(event);

                    let mut internals = shared.internals.lock();
                    internals.last_error = None;
                    let now = Instant::now();
                    let conn = IdleConn {
                        conn: Conn {
                            conn,
                            extensions: Extensions::new(),
                            birth: now,
                            id,
                        },
                        idle_start: now,
                    };
                    internals.conns.push(conn);
                    internals.pending_conns -= 1;
                    internals.num_conns += 1;
                    shared.cond.notify_one();
                }
                Err(err) => {
                    shared.internals.lock().last_error = Some(err.to_string());
                    shared.config.error_handler.handle_error(err);
                    let delay = cmp::max(Duration::from_millis(200), delay);
                    let delay = cmp::min(shared.config.connection_timeout / 2, delay * 2);
                    inner(delay, &shared);
                }
            }
        });
    }
}

fn reap_connections<M>(shared: &Weak<SharedPool<M>>)
where
    M: ManageConnection,
{
    let shared = match shared.upgrade() {
        Some(shared) => shared,
        None => return,
    };

    let mut old = Vec::with_capacity(shared.config.max_size as usize);
    let mut to_drop = vec![];

    let mut internals = shared.internals.lock();
    mem::swap(&mut old, &mut internals.conns);
    let now = Instant::now();
    for conn in old {
        let mut reap = false;
        if let Some(timeout) = shared.config.idle_timeout {
            reap |= now - conn.idle_start >= timeout;
        }
        if let Some(lifetime) = shared.config.max_lifetime {
            reap |= now - conn.conn.birth >= lifetime;
        }
        if reap {
            to_drop.push(conn.conn);
        } else {
            internals.conns.push(conn);
        }
    }
    drop_conns(&shared, internals, to_drop);
}

/// A generic connection pool.
pub struct Pool<M>(Arc<SharedPool<M>>)
where
    M: ManageConnection;

/// Returns a new `Pool` referencing the same state as `self`.
impl<M> Clone for Pool<M>
where
    M: ManageConnection,
{
    fn clone(&self) -> Pool<M> {
        Pool(self.0.clone())
    }
}

impl<M> fmt::Debug for Pool<M>
where
    M: ManageConnection + fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Pool")
            .field("state", &self.state())
            .field("config", &self.0.config)
            .field("manager", &self.0.manager)
            .finish()
    }
}

impl<M> Pool<M>
where
    M: ManageConnection,
{
    /// Creates a new connection pool with a default configuration.
    pub fn new(manager: M) -> Result<Pool<M>, Error> {
        Pool::builder().build(manager)
    }

    /// Returns a builder type to configure a new pool.
    pub fn builder() -> Builder<M> {
        Builder::new()
    }

    // for testing
    fn new_inner(
        config: Config<M::Connection, M::Error>,
        manager: M,
        reaper_rate: Duration,
    ) -> Pool<M> {
        let internals = PoolInternals {
            conns: Vec::with_capacity(config.max_size as usize),
            num_conns: 0,
            pending_conns: 0,
            last_error: None,
        };

        let shared = Arc::new(SharedPool {
            config,
            manager,
            internals: Mutex::new(internals),
            cond: Condvar::new(),
        });

        establish_idle_connections(&shared, &mut shared.internals.lock());

        if shared.config.max_lifetime.is_some() || shared.config.idle_timeout.is_some() {
            let s = Arc::downgrade(&shared);
            shared
                .config
                .thread_pool
                .execute_at_fixed_rate(reaper_rate, reaper_rate, move || reap_connections(&s));
        }

        Pool(shared)
    }

    fn wait_for_initialization(&self) -> Result<(), Error> {
        let end = Instant::now() + self.0.config.connection_timeout;
        let mut internals = self.0.internals.lock();

        let initial_size = self.0.config.min_idle.unwrap_or(self.0.config.max_size);

        while internals.num_conns != initial_size {
            if self.0.cond.wait_until(&mut internals, end).timed_out() {
                return Err(Error(internals.last_error.take()));
            }
        }

        Ok(())
    }

    /// Retrieves a connection from the pool.
    ///
    /// Waits for at most the configured connection timeout before returning an
    /// error.
    pub fn get(&self) -> Result<PooledConnection<M>, Error> {
        self.get_timeout(self.0.config.connection_timeout)
    }

    /// Retrieves a connection from the pool, waiting for at most `timeout`
    ///
    /// The given timeout will be used instead of the configured connection
    /// timeout.
    pub fn get_timeout(&self, timeout: Duration) -> Result<PooledConnection<M>, Error> {
        let start = Instant::now();
        let end = start + timeout;
        let mut internals = self.0.internals.lock();

        loop {
            match self.try_get_inner(internals) {
                Ok(conn) => {
                    let event = CheckoutEvent {
                        id: conn.conn.as_ref().unwrap().id,
                        duration: start.elapsed(),
                    };
                    self.0.config.event_handler.handle_checkout(event);
                    return Ok(conn);
                }
                Err(i) => internals = i,
            }

            add_connection(&self.0, &mut internals);

            if self.0.cond.wait_until(&mut internals, end).timed_out() {
                let event = TimeoutEvent { timeout };
                self.0.config.event_handler.handle_timeout(event);

                return Err(Error(internals.last_error.take()));
            }
        }
    }

    /// Attempts to retrieve a connection from the pool if there is one
    /// available.
    ///
    /// Returns `None` if there are no idle connections available in the pool.
    /// This method will not block waiting to establish a new connection.
    pub fn try_get(&self) -> Option<PooledConnection<M>> {
        self.try_get_inner(self.0.internals.lock()).ok()
    }

    fn try_get_inner<'a>(
        &'a self,
        mut internals: MutexGuard<'a, PoolInternals<M::Connection>>,
    ) -> Result<PooledConnection<M>, MutexGuard<'a, PoolInternals<M::Connection>>> {
        loop {
            if let Some(mut conn) = internals.conns.pop() {
                establish_idle_connections(&self.0, &mut internals);
                drop(internals);

                if self.0.config.test_on_check_out {
                    if let Err(e) = self.0.manager.is_valid(&mut conn.conn.conn) {
                        let msg = e.to_string();
                        self.0.config.error_handler.handle_error(e);
                        // FIXME we shouldn't have to lock, unlock, and relock here
                        internals = self.0.internals.lock();
                        internals.last_error = Some(msg);
                        drop_conns(&self.0, internals, vec![conn.conn]);
                        internals = self.0.internals.lock();
                        continue;
                    }
                }

                return Ok(PooledConnection {
                    pool: self.clone(),
                    checkout: Instant::now(),
                    conn: Some(conn.conn),
                });
            } else {
                return Err(internals);
            }
        }
    }

    fn put_back(&self, checkout: Instant, mut conn: Conn<M::Connection>) {
        let event = CheckinEvent {
            id: conn.id,
            duration: checkout.elapsed(),
        };
        self.0.config.event_handler.handle_checkin(event);

        // This is specified to be fast, but call it before locking anyways
        let broken = self.0.manager.has_broken(&mut conn.conn);

        let mut internals = self.0.internals.lock();
        if broken {
            drop_conns(&self.0, internals, vec![conn]);
        } else {
            let conn = IdleConn {
                conn,
                idle_start: Instant::now(),
            };
            internals.conns.push(conn);
            self.0.cond.notify_one();
        }
    }

    /// Returns information about the current state of the pool.
    pub fn state(&self) -> State {
        let internals = self.0.internals.lock();
        State {
            connections: internals.num_conns,
            idle_connections: internals.conns.len() as u32,
            _p: (),
        }
    }

    /// Returns the configured maximum pool size.
    pub fn max_size(&self) -> u32 {
        self.0.config.max_size
    }

    /// Returns the configured mimimum idle connection count.
    pub fn min_idle(&self) -> Option<u32> {
        self.0.config.min_idle
    }

    /// Returns if the pool is configured to test connections on check out.
    pub fn test_on_check_out(&self) -> bool {
        self.0.config.test_on_check_out
    }

    /// Returns the configured maximum connection lifetime.
    pub fn max_lifetime(&self) -> Option<Duration> {
        self.0.config.max_lifetime
    }

    /// Returns the configured idle connection timeout.
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.0.config.idle_timeout
    }

    /// Returns the configured connection timeout.
    pub fn connection_timeout(&self) -> Duration {
        self.0.config.connection_timeout
    }
}

/// The error type returned by methods in this crate.
#[derive(Debug)]
pub struct Error(Option<String>);

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(error::Error::description(self))?;
        if let Some(ref err) = self.0 {
            write!(fmt, ": {}", err)?;
        }
        Ok(())
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        "timed out waiting for connection"
    }
}

/// Information about the state of a `Pool`.
pub struct State {
    /// The number of connections currently being managed by the pool.
    pub connections: u32,
    /// The number of idle connections.
    pub idle_connections: u32,
    _p: (),
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("State")
            .field("connections", &self.connections)
            .field("idle_connections", &self.idle_connections)
            .finish()
    }
}

/// A smart pointer wrapping a connection.
pub struct PooledConnection<M>
where
    M: ManageConnection,
{
    pool: Pool<M>,
    checkout: Instant,
    conn: Option<Conn<M::Connection>>,
}

impl<M> fmt::Debug for PooledConnection<M>
where
    M: ManageConnection,
    M::Connection: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.conn.as_ref().unwrap().conn, fmt)
    }
}

impl<M> Drop for PooledConnection<M>
where
    M: ManageConnection,
{
    fn drop(&mut self) {
        self.pool.put_back(self.checkout, self.conn.take().unwrap());
    }
}

impl<M> Deref for PooledConnection<M>
where
    M: ManageConnection,
{
    type Target = M::Connection;

    fn deref(&self) -> &M::Connection {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<M> DerefMut for PooledConnection<M>
where
    M: ManageConnection,
{
    fn deref_mut(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<M> PooledConnection<M>
where
    M: ManageConnection,
{
    /// Returns a shared reference to the extensions associated with this connection.
    pub fn extensions(this: &Self) -> &Extensions {
        &this.conn.as_ref().unwrap().extensions
    }

    /// Returns a mutable reference to the extensions associated with this connection.
    pub fn extensions_mut(this: &mut Self) -> &mut Extensions {
        &mut this.conn.as_mut().unwrap().extensions
    }
}
