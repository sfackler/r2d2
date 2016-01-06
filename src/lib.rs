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
//!         .error_handler(Box::new(r2d2::LoggingErrorHandler))
//!         .build();
//!     let manager = r2d2_foodb::FooConnectionManager::new("localhost:1234");
//!
//!     let pool = r2d2::Pool::new(config, manager).unwrap();
//!
//!     for _ in 0..20i32 {
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
#![doc(html_root_url="https://sfackler.github.io/r2d2/doc/v0.6.1")]

#[macro_use]
extern crate log;
extern crate time;

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::mem;
use std::sync::{Arc, Mutex, Condvar};
use time::{Duration, SteadyTime};

#[doc(inline)]
pub use config::Config;

use task::ScheduledThreadPool;

pub mod config;
mod task;
mod thunk;

/// A trait which provides connection-specific functionality.
pub trait ManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;

    /// The error type returned by `Connection`s.
    type Error: 'static;

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
pub trait HandleError<E>: Send+Sync+'static {
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

impl<E> HandleError<E> for LoggingErrorHandler where E: Error {
    fn handle_error(&self, error: E) {
        error!("{}", error);
    }
}

/// A trait which allows for customization of connections.
pub trait CustomizeConnection<C, E>: Send+Sync+'static {
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
    birth: SteadyTime,
    idle_start: SteadyTime,
}

impl<C> Conn<C> {
    fn new(conn: C) -> Conn<C> {
        Conn {
            conn: conn,
            birth: SteadyTime::now(),
            idle_start: SteadyTime::now(),
        }
    }
}

struct PoolInternals<C> {
    conns: VecDeque<Conn<C>>,
    num_conns: u32,
    pending_conns: u32,
}

struct SharedPool<M> where M: ManageConnection {
    config: Config<M::Connection, M::Error>,
    manager: M,
    internals: Mutex<PoolInternals<M::Connection>>,
    cond: Condvar,
    thread_pool: ScheduledThreadPool,
}

fn drop_conn<M>(shared: &Arc<SharedPool<M>>,
                internals: &mut PoolInternals<M::Connection>)
        where M: ManageConnection
{
    internals.num_conns -= 1;

    let min = shared.config.min_idle().unwrap_or(shared.config.pool_size());
    if internals.num_conns + internals.pending_conns < min {
        add_connection(shared, internals);
    }
}

fn add_connection<M>(shared: &Arc<SharedPool<M>>,
                     internals: &mut PoolInternals<M::Connection>)
    where M: ManageConnection
{
    internals.pending_conns += 1;
    inner(Duration::zero(), shared);

    fn inner<M>(delay: Duration, shared: &Arc<SharedPool<M>>) where M: ManageConnection {
        let new_shared = shared.clone();
        shared.thread_pool.run_after(delay, move || {
            let shared = new_shared;
            let conn = shared.manager.connect().and_then(|mut conn| {
                shared.config.connection_customizer().on_acquire(&mut conn).map(|_| conn)
            });
            match conn {
                Ok(conn) => {
                    let mut internals = shared.internals.lock().unwrap();
                    internals.conns.push_back(Conn::new(conn));
                    internals.pending_conns -= 1;
                    internals.num_conns += 1;
                    shared.cond.notify_one();
                }
                Err(err) => {
                    shared.config.error_handler().handle_error(err);
                    inner(Duration::seconds(1), &shared);
                },
            }
        });
    }
}

fn reap_connections<M>(shared: &Arc<SharedPool<M>>) where M: ManageConnection {
    let mut old = VecDeque::with_capacity(shared.config.pool_size() as usize);
    let mut to_drop = vec![];

    let mut internals = shared.internals.lock().unwrap();
    mem::swap(&mut old, &mut internals.conns);
    let now = SteadyTime::now();
    for conn in old {
        let mut reap = false;
        if let Some(timeout) = shared.config.idle_timeout() {
            reap |= now - conn.idle_start >= cvt(timeout);
        }
        if let Some(lifetime) = shared.config.max_lifetime() {
            reap |= now - conn.birth >= cvt(lifetime);
        }
        if reap {
            drop_conn(shared, &mut internals);
            to_drop.push(conn.conn);
        } else {
            internals.conns.push_back(conn);
        }
    }
    drop(internals); // make sure we run to_drop destructors without this locked
}

/// A generic connection pool.
pub struct Pool<M> where M: ManageConnection {
    shared: Arc<SharedPool<M>>,
}

impl<M> Drop for Pool<M> where M: ManageConnection {
    fn drop(&mut self) {
        self.shared.thread_pool.clear();
    }
}

/// Returns a new `Pool` referencing the same state as `self`.
impl<M> Clone for Pool<M> where M: ManageConnection {
    fn clone(&self) -> Pool<M> {
        Pool {
            shared: self.shared.clone(),
        }
    }
}

impl<M> fmt::Debug for Pool<M> where M: ManageConnection + fmt::Debug {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Pool")
            .field("idle_connections", &self.shared.internals.lock().unwrap().conns.len())
            .field("config", &self.shared.config)
            .field("manager", &self.shared.manager)
            .finish()
    }
}

/// An error returned by `Pool::new` if it fails to initialize connections.
#[derive(Debug)]
pub struct InitializationError(());

impl fmt::Display for InitializationError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(self.description())
    }
}

impl Error for InitializationError {
    fn description(&self) -> &str {
        "Unable to initialize connections"
    }
}

/// An error returned by `Pool::get` if it times out without retrieving a connection.
#[derive(Debug)]
pub struct GetTimeout(());

impl fmt::Display for GetTimeout {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(self.description())
    }
}

impl Error for GetTimeout {
    fn description(&self) -> &str {
        "Timed out while waiting for a connection"
    }
}

impl<M> Pool<M> where M: ManageConnection {
    /// Creates a new connection pool.
    ///
    /// Returns an `Err` value if `initialization_fail_fast` is set to true in
    /// the configuration and the pool is unable to open all of its
    /// connections.
    pub fn new(config: Config<M::Connection, M::Error>, manager: M)
               -> Result<Pool<M>, InitializationError> {
        let internals = PoolInternals {
            conns: VecDeque::with_capacity(config.pool_size() as usize),
            num_conns: 0,
            pending_conns: 0,
        };

        let shared = Arc::new(SharedPool {
            thread_pool: ScheduledThreadPool::new(config.helper_threads() as usize),
            config: config,
            manager: manager,
            internals: Mutex::new(internals),
            cond: Condvar::new(),
        });

        {
            let mut inner = shared.internals.lock().unwrap();
            for _ in 0..shared.config.min_idle().unwrap_or(shared.config.pool_size()) {
                add_connection(&shared, &mut inner);
            }
            drop(inner);
        }

        if shared.config.initialization_fail_fast() {
            let end = SteadyTime::now() + cvt(shared.config.connection_timeout());
            let mut internals = shared.internals.lock().unwrap();

            while internals.num_conns != shared.config.pool_size() {
                let wait = end - SteadyTime::now();
                if wait <= Duration::zero() {
                    return Err(InitializationError(()));
                }
                internals = shared.cond.wait_timeout_ms(internals,
                                                        wait.num_milliseconds() as u32)
                    .unwrap().0;
            }
        }

        if shared.config.max_lifetime().is_some() || shared.config.idle_timeout().is_some() {
            let s = shared.clone();
            shared.thread_pool.run_at_fixed_rate(Duration::seconds(30),
                                                 move || reap_connections(&s));
        }

        Ok(Pool {
            shared: shared,
        })
    }

    fn get_inner(&self) -> Result<Conn<M::Connection>, GetTimeout> {
        let end = SteadyTime::now() + cvt(self.shared.config.connection_timeout());
        let mut internals = self.shared.internals.lock().unwrap();

        loop {
            match internals.conns.pop_front() {
                Some(mut conn) => {
                    drop(internals);

                    if self.shared.config.test_on_check_out() {
                        if let Err(e) = self.shared.manager.is_valid(&mut conn.conn) {
                            self.shared.config.error_handler().handle_error(e);
                            internals = self.shared.internals.lock().unwrap();
                            drop_conn(&self.shared, &mut internals);
                            continue;
                        }
                    }

                    return Ok(conn);
                }
                None => {
                    if internals.num_conns + internals.pending_conns < self.shared.config.pool_size() {
                        add_connection(&self.shared, &mut internals);
                    }

                    let now = SteadyTime::now();
                    let mut timeout = (end - now).num_milliseconds();
                    if timeout < 0 {
                        timeout = 0
                    };
                    let (new_internals, no_timeout) =
                        self.shared.cond.wait_timeout_ms(internals, timeout as u32).unwrap();
                    internals = new_internals;

                    if !no_timeout {
                        return Err(GetTimeout(()));
                    }
                }
            }
        }
    }

    /// Retrieves a connection from the pool.
    ///
    /// Waits for at most `Config::connection_timeout` before returning an
    /// error.
    pub fn get(&self) -> Result<PooledConnection<M>, GetTimeout> {
        Ok(PooledConnection {
            pool: self.clone(),
            conn: Some(try!(self.get_inner())),
        })
    }

    fn put_back(&self, mut conn: Conn<M::Connection>) {
        // This is specified to be fast, but call it before locking anyways
        let broken = self.shared.manager.has_broken(&mut conn.conn);

        let mut internals = self.shared.internals.lock().unwrap();
        if broken {
            drop_conn(&self.shared, &mut internals);
        } else {
            conn.idle_start = SteadyTime::now();
            internals.conns.push_back(conn);
            self.shared.cond.notify_one();
        }
    }
}

/// A smart pointer wrapping a connection.
pub struct PooledConnection<M> where M: ManageConnection {
    pool: Pool<M>,
    conn: Option<Conn<M::Connection>>,
}

impl<M> fmt::Debug for PooledConnection<M>
        where M: ManageConnection + fmt::Debug, M::Connection: fmt::Debug {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("PooledConnection")
            .field("pool", &self.pool)
            .field("connection", &self.conn.as_ref().unwrap().conn)
            .finish()
    }
}

impl<M> Drop for PooledConnection<M> where M: ManageConnection {
    fn drop(&mut self) {
        self.pool.put_back(self.conn.take().unwrap());
    }
}

impl<M> Deref for PooledConnection<M> where M: ManageConnection {
    type Target = M::Connection;

    fn deref(&self) -> &M::Connection {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<M> DerefMut for PooledConnection<M> where M: ManageConnection {
    fn deref_mut(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
    }
}

fn cvt(d: std::time::Duration) -> Duration {
    Duration::seconds(d.as_secs() as i64) + Duration::nanoseconds(d.subsec_nanos() as i64)
}
