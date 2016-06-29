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
//!     let config = r2d2::Config::builder()
//!         .pool_size(15)
//!         .build();
//!     let manager = r2d2_foodb::FooConnectionManager::new("localhost:1234");
//!
//!     let pool = r2d2::Pool::new(config, manager).unwrap();
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
#![doc(html_root_url="https://sfackler.github.io/r2d2/doc/v0.7.0")]

extern crate antidote;
#[macro_use]
extern crate log;

use antidote::{Mutex, Condvar};
use std::cmp;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::mem;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

#[doc(inline)]
pub use config::Config;

use task::ScheduledThreadPool;

pub mod config;
mod task;
mod thunk;

#[cfg(test)]
mod test;

/// A trait which provides connection-specific functionality.
pub trait ManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;

    /// The error type returned by `Connection`s.
    type Error: Error + 'static;

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
    where E: Error
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
}

/// A `CustomizeConnection` which does nothing.
#[derive(Copy, Clone, Debug)]
pub struct NopConnectionCustomizer;

impl<C, E> CustomizeConnection<C, E> for NopConnectionCustomizer {}

struct Conn<C> {
    conn: C,
    birth: Instant,
    idle_start: Instant,
}

impl<C> Conn<C> {
    fn new(conn: C) -> Conn<C> {
        let now = Instant::now();
        Conn {
            conn: conn,
            birth: now,
            idle_start: now,
        }
    }
}

struct PoolInternals<C> {
    conns: VecDeque<Conn<C>>,
    num_conns: u32,
    pending_conns: u32,
    last_error: Option<String>,
}

struct SharedPool<M>
    where M: ManageConnection
{
    config: Config<M::Connection, M::Error>,
    manager: M,
    internals: Mutex<PoolInternals<M::Connection>>,
    cond: Condvar,
    thread_pool: ScheduledThreadPool,
}

fn drop_conn<M>(shared: &Arc<SharedPool<M>>, internals: &mut PoolInternals<M::Connection>)
    where M: ManageConnection
{
    internals.num_conns -= 1;

    let min = shared.config.min_idle().unwrap_or(shared.config.pool_size());
    if internals.num_conns + internals.pending_conns < min {
        add_connection(shared, internals);
    }
}

fn add_connection<M>(shared: &Arc<SharedPool<M>>, internals: &mut PoolInternals<M::Connection>)
    where M: ManageConnection
{
    internals.pending_conns += 1;
    inner(Duration::from_secs(0), shared);

    fn inner<M>(delay: Duration, shared: &Arc<SharedPool<M>>)
        where M: ManageConnection
    {
        let new_shared = Arc::downgrade(shared);
        shared.thread_pool.run_after(delay, move || {
            let shared = match new_shared.upgrade() {
                Some(shared) => shared,
                None => return,
            };

            let conn = shared.manager.connect().and_then(|mut conn| {
                shared.config.connection_customizer().on_acquire(&mut conn).map(|_| conn)
            });
            match conn {
                Ok(conn) => {
                    let mut internals = shared.internals.lock();
                    internals.last_error = None;
                    internals.conns.push_back(Conn::new(conn));
                    internals.pending_conns -= 1;
                    internals.num_conns += 1;
                    shared.cond.notify_one();
                }
                Err(err) => {
                    shared.internals.lock().last_error = Some(err.to_string());
                    shared.config.error_handler().handle_error(err);
                    let delay = cmp::max(Duration::from_millis(200), delay);
                    let delay = cmp::min(shared.config.connection_timeout() / 2, delay * 2);
                    inner(delay, &shared);
                }
            }
        });
    }
}

fn reap_connections<M>(shared: &Weak<SharedPool<M>>)
    where M: ManageConnection
{
    let shared = match shared.upgrade() {
        Some(shared) => shared,
        None => return,
    };

    let mut old = VecDeque::with_capacity(shared.config.pool_size() as usize);
    let mut to_drop = vec![];

    let mut internals = shared.internals.lock();
    mem::swap(&mut old, &mut internals.conns);
    let now = Instant::now();
    for conn in old {
        let mut reap = false;
        if let Some(timeout) = shared.config.idle_timeout() {
            reap |= now - conn.idle_start >= timeout;
        }
        if let Some(lifetime) = shared.config.max_lifetime() {
            reap |= now - conn.birth >= lifetime;
        }
        if reap {
            drop_conn(&shared, &mut internals);
            to_drop.push(conn.conn);
        } else {
            internals.conns.push_back(conn);
        }
    }
    drop(internals); // make sure we run to_drop destructors without this locked
}

/// A generic connection pool.
pub struct Pool<M: ManageConnection>(Arc<SharedPool<M>>);

/// Returns a new `Pool` referencing the same state as `self`.
impl<M> Clone for Pool<M>
    where M: ManageConnection
{
    fn clone(&self) -> Pool<M> {
        Pool(self.0.clone())
    }
}

impl<M> fmt::Debug for Pool<M>
    where M: ManageConnection + fmt::Debug
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let inner = self.0.internals.lock();

        fmt.debug_struct("Pool")
           .field("connections", &inner.num_conns)
           .field("idle_connections", &inner.conns.len())
           .field("config", &self.0.config)
           .field("manager", &self.0.manager)
           .finish()
    }
}

impl<M> Pool<M>
    where M: ManageConnection
{
    /// Creates a new connection pool.
    ///
    /// Returns an `Err` value if `initialization_fail_fast` is set to true in
    /// the configuration and the pool is unable to open all of its
    /// connections.
    pub fn new(config: Config<M::Connection, M::Error>,
               manager: M)
               -> Result<Pool<M>, InitializationError> {
        Pool::new_inner(config, manager, Duration::from_secs(30))
    }

    // for testing
    fn new_inner(config: Config<M::Connection, M::Error>,
                 manager: M,
                 reaper_rate: Duration)
                 -> Result<Pool<M>, InitializationError> {
        let internals = PoolInternals {
            conns: VecDeque::with_capacity(config.pool_size() as usize),
            num_conns: 0,
            pending_conns: 0,
            last_error: None,
        };

        let shared = Arc::new(SharedPool {
            thread_pool: ScheduledThreadPool::new(config.helper_threads() as usize),
            config: config,
            manager: manager,
            internals: Mutex::new(internals),
            cond: Condvar::new(),
        });

        let initial_size = shared.config.min_idle().unwrap_or(shared.config.pool_size());
        {
            let mut inner = shared.internals.lock();
            for _ in 0..initial_size {
                add_connection(&shared, &mut inner);
            }
            drop(inner);
        }

        if shared.config.initialization_fail_fast() {
            let end = Instant::now() + shared.config.connection_timeout();
            let mut internals = shared.internals.lock();

            while internals.num_conns != initial_size {
                let now = Instant::now();
                if now >= end {
                    return Err(InitializationError(internals.last_error.take()));
                }
                internals = shared.cond
                                  .wait_timeout(internals, end - now)
                                  .0;
            }
        }

        if shared.config.max_lifetime().is_some() || shared.config.idle_timeout().is_some() {
            let s = Arc::downgrade(&shared);
            shared.thread_pool
                  .run_at_fixed_rate(reaper_rate, move || reap_connections(&s));
        }

        Ok(Pool(shared))
    }

    /// Retrieves a connection from the pool.
    ///
    /// Waits for at most `Config::connection_timeout` before returning an
    /// error.
    pub fn get(&self) -> Result<PooledConnection<M>, GetTimeout> {
        let end = Instant::now() + self.0.config.connection_timeout();
        let mut internals = self.0.internals.lock();

        let connection;
        loop {
            match internals.conns.pop_front() {
                Some(mut conn) => {
                    drop(internals);

                    if self.0.config.test_on_check_out() {
                        if let Err(e) = self.0.manager.is_valid(&mut conn.conn) {
                            let msg = e.to_string();
                            self.0.config.error_handler().handle_error(e);
                            internals = self.0.internals.lock();
                            internals.last_error = Some(msg);
                            drop_conn(&self.0, &mut internals);
                            continue;
                        }
                    }

                    connection = conn;
                    break;
                }
                None => {
                    if internals.num_conns + internals.pending_conns < self.0.config.pool_size() {
                        add_connection(&self.0, &mut internals);
                    }

                    let now = Instant::now();
                    if now >= end {
                        return Err(GetTimeout(internals.last_error.take()));
                    };
                    internals = self.0
                                    .cond
                                    .wait_timeout(internals, end - now)
                                    .0;
                }
            }
        }

        Ok(PooledConnection {
            pool: self.clone(),
            conn: Some(connection),
        })
    }

    fn put_back(&self, mut conn: Conn<M::Connection>) {
        // This is specified to be fast, but call it before locking anyways
        let broken = self.0.manager.has_broken(&mut conn.conn);

        let mut internals = self.0.internals.lock();
        if broken {
            drop_conn(&self.0, &mut internals);
        } else {
            conn.idle_start = Instant::now();
            internals.conns.push_back(conn);
            self.0.cond.notify_one();
        }
    }
}

/// An error returned by `Pool::new` if it fails to initialize connections.
#[derive(Debug)]
pub struct InitializationError(Option<String>);

impl fmt::Display for InitializationError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(fmt.write_str(self.description()));
        if let Some(ref err) = self.0 {
            try!(write!(fmt, ": {}", err));
        }
        Ok(())
    }
}

impl Error for InitializationError {
    fn description(&self) -> &str {
        "Unable to initialize connections"
    }
}

/// An error returned by `Pool::get` if it times out without retrieving a connection.
#[derive(Debug)]
pub struct GetTimeout(Option<String>);

impl fmt::Display for GetTimeout {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(fmt.write_str(self.description()));
        if let Some(ref err) = self.0 {
            try!(write!(fmt, ": {}", err));
        }
        Ok(())
    }
}

impl Error for GetTimeout {
    fn description(&self) -> &str {
        "Timed out while waiting for a connection"
    }
}

/// A smart pointer wrapping a connection.
pub struct PooledConnection<M>
    where M: ManageConnection
{
    pool: Pool<M>,
    conn: Option<Conn<M::Connection>>,
}

impl<M> fmt::Debug for PooledConnection<M>
    where M: ManageConnection,
          M::Connection: fmt::Debug
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.conn.as_ref().unwrap().conn, fmt)
    }
}

impl<M> Drop for PooledConnection<M>
    where M: ManageConnection
{
    fn drop(&mut self) {
        self.pool.put_back(self.conn.take().unwrap());
    }
}

impl<M> Deref for PooledConnection<M>
    where M: ManageConnection
{
    type Target = M::Connection;

    fn deref(&self) -> &M::Connection {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<M> DerefMut for PooledConnection<M>
    where M: ManageConnection
{
    fn deref_mut(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
    }
}
