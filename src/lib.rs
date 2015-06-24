//! A library providing a generic connection pool.
#![warn(missing_docs)]
#![doc(html_root_url="https://sfackler.github.io/r2d2/doc/v0.5.8")]

#[macro_use]
extern crate log;
extern crate time;
extern crate debug_builders;

use debug_builders::DebugStruct;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard, Condvar};
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
pub struct NoopErrorHandler;

impl<E> HandleError<E> for NoopErrorHandler {
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
pub struct NoopConnectionCustomizer;

impl<C, E> CustomizeConnection<C, E> for NoopConnectionCustomizer {}

struct PoolInternals<C> {
    conns: VecDeque<C>,
    num_conns: u32,
}

struct SharedPool<M> where M: ManageConnection {
    config: Config<M::Connection, M::Error>,
    manager: M,
    internals: Mutex<PoolInternals<M::Connection>>,
    cond: Condvar,
    thread_pool: ScheduledThreadPool,
}

fn add_connection<M>(delay: Duration, shared: &Arc<SharedPool<M>>) where M: ManageConnection {
    let new_shared = shared.clone();
    shared.thread_pool.run_after(delay, move || {
        let shared = new_shared;
        let conn = shared.manager.connect().and_then(|mut conn| {
            shared.config.connection_customizer().on_acquire(&mut conn).map(|_| conn)
        });
        match conn {
            Ok(conn) => {
                let mut internals = shared.internals.lock().unwrap();
                internals.conns.push_back(conn);
                internals.num_conns += 1;
                shared.cond.notify_one();
            }
            Err(err) => {
                shared.config.error_handler().handle_error(err);
                add_connection(Duration::seconds(1), &shared);
            },
        }
    });
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

impl<M> Clone for Pool<M> where M: ManageConnection {
    /// Returns a new `Pool` referencing the same state as `self`.
    fn clone(&self) -> Pool<M> {
        Pool {
            shared: self.shared.clone(),
        }
    }
}

impl<M> fmt::Debug for Pool<M> where M: ManageConnection + fmt::Debug {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        DebugStruct::new(fmt, "Pool")
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
            conns: VecDeque::new(),
            num_conns: 0,
        };

        let shared = Arc::new(SharedPool {
            thread_pool: ScheduledThreadPool::new(config.helper_threads() as usize),
            config: config,
            manager: manager,
            internals: Mutex::new(internals),
            cond: Condvar::new(),
        });

        for _ in 0..shared.config.pool_size() {
            add_connection(Duration::zero(), &shared);
        }

        if shared.config.initialization_fail_fast() {
            let end = SteadyTime::now() +
                Duration::milliseconds(shared.config.connection_timeout_ms() as i64);
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

        Ok(Pool {
            shared: shared,
        })
    }

    fn get_inner(&self) -> Result<M::Connection, GetTimeout> {
        let end = SteadyTime::now() +
            Duration::milliseconds(self.shared.config.connection_timeout_ms() as i64);
        let mut internals = self.shared.internals.lock().unwrap();

        loop {
            match internals.conns.pop_front() {
                Some(mut conn) => {
                    drop(internals);

                    if self.shared.config.test_on_check_out() {
                        if let Err(e) = self.shared.manager.is_valid(&mut conn) {
                            self.shared.config.error_handler().handle_error(e);
                            internals = self.shared.internals.lock().unwrap();
                            self.handle_broken(&mut internals);
                            continue
                        }
                    }

                    return Ok(conn);
                }
                None => {
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

    fn handle_broken(&self, internals: &mut MutexGuard<PoolInternals<M::Connection>>) {
        internals.num_conns -= 1;
        add_connection(Duration::zero(), &self.shared);
    }

    fn put_back(&self, mut conn: M::Connection) {
        // This is specified to be fast, but call it before locking anyways
        let broken = self.shared.manager.has_broken(&mut conn);

        let mut internals = self.shared.internals.lock().unwrap();
        if broken {
            self.handle_broken(&mut internals);
        } else {
            internals.conns.push_back(conn);
            self.shared.cond.notify_one();
        }
    }
}

/// A smart pointer wrapping a connection.
pub struct PooledConnection<M> where M: ManageConnection {
    pool: Pool<M>,
    conn: Option<M::Connection>,
}

impl<M> fmt::Debug for PooledConnection<M>
        where M: ManageConnection + fmt::Debug, M::Connection: fmt::Debug {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        DebugStruct::new(fmt, "PooledConnection")
            .field("pool", &self.pool)
            .field("connection", self.conn.as_ref().unwrap())
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
        self.conn.as_ref().unwrap()
    }
}

impl<M> DerefMut for PooledConnection<M> where M: ManageConnection {
    fn deref_mut(&mut self) -> &mut M::Connection {
        self.conn.as_mut().unwrap()
    }
}
