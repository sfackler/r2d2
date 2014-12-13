//! A library providing a generic connection pool.

#![feature(unsafe_destructor, phase)]
#![warn(missing_docs)]
#![doc(html_root_url="https://sfackler.github.io/doc")]

#[phase(plugin, link)]
extern crate log;

use std::collections::RingBuf;
use std::sync::{Arc, Mutex, Condvar, TaskPool};
use std::fmt;

pub use config::{Config, ConfigError};

mod config;

/// A trait which provides database-specific functionality.
pub trait PoolManager<C, E>: Send+Sync {
    /// Attempts to create a new connection.
    fn connect(&self) -> Result<C, E>;

    /// Determines if the connection is still connected to the database.
    ///
    /// A standard implementation would check if a simple query like `SELECT 1`
    /// succeeds.
    fn is_valid(&self, conn: &mut C) -> Result<(), E>;

    /// *Quickly* determines if the connection is no longer usable.
    ///
    /// This will be called synchronously every time a connection is returned
    /// to the pool, so it should *not* block. If it returns `true`, the
    /// connection will be discarded.
    ///
    /// For example, an implementation might check if the underlying TCP socket
    /// has disconnected. Implementations that do not support this kind of
    /// fast health check may simply return `false`.
    fn has_broken(&self, conn: &mut C) -> bool;
}

/// A trait which handles errors reported by the `PoolManager`.
pub trait ErrorHandler<E>: Send+Sync {
    /// Handles an error.
    fn handle_error(&self, error: E);
}

impl<E> ErrorHandler<E> for Box<ErrorHandler<E>> {
    fn handle_error(&self, error: E) {
        (**self).handle_error(error)
    }
}

/// An `ErrorHandler` which does nothing.
#[deriving(Copy)]
pub struct NoopErrorHandler;

impl<E> ErrorHandler<E> for NoopErrorHandler {
    fn handle_error(&self, _: E) {}
}

/// An `ErrorHandler` which logs at the error level.
#[deriving(Copy)]
pub struct LoggingErrorHandler;

impl<E> ErrorHandler<E> for LoggingErrorHandler where E: fmt::Show {
    fn handle_error(&self, error: E) {
        error!("{}", error);
    }
}

struct PoolInternals<C> {
    conns: RingBuf<C>,
    num_conns: uint,
    task_pool: TaskPool,
}

struct InnerPool<C, E, M, H> where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    config: Config,
    manager: M,
    error_handler: H,
    internals: Mutex<PoolInternals<C>>,
    cond: Condvar,
}

fn add_connection<C, E, M, H>(inner: &Arc<InnerPool<C, E, M, H>>)
        where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    let new_inner = inner.clone();
    inner.internals.lock().task_pool.execute(proc() {
        let inner = new_inner;
        match inner.manager.connect() {
            Ok(conn) => {
                let mut internals = inner.internals.lock();
                internals.conns.push_back(conn);
                internals.num_conns += 1;
                inner.cond.notify_one();
            }
            Err(err) => inner.error_handler.handle_error(err),
        }
    });
}

/// A generic connection pool.
pub struct Pool<C, E, M, H> where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    inner: Arc<InnerPool<C, E, M, H>>,
}

impl<C, E, M, H> Pool<C, E, M, H>
        where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    /// Creates a new connection pool.
    ///
    /// Returns an `Err` value only if `config` is invalid.
    pub fn new(config: Config, manager: M, error_handler: H)
               -> Result<Pool<C, E, M, H>, ConfigError> {
        try!(config.validate());

        let internals = PoolInternals {
            conns: RingBuf::new(),
            num_conns: config.pool_size,
            task_pool: TaskPool::new(config.helper_tasks),
        };

        let inner = Arc::new(InnerPool {
            config: config,
            manager: manager,
            error_handler: error_handler,
            internals: Mutex::new(internals),
            cond: Condvar::new(),
        });

        for _ in range(0, config.pool_size) {
            add_connection(&inner);
        }

        Ok(Pool {
            inner: inner,
        })
    }

    /// Retrieves a connection from the pool.
    pub fn get<'a>(&'a self) -> Result<PooledConnection<'a, C, E, M, H>, ()> {
        let mut internals = self.inner.internals.lock();

        loop {
            match internals.conns.pop_front() {
                Some(mut conn) => {
                    drop(internals);

                    if self.inner.config.test_on_check_out {
                        if let Err(e) = self.inner.manager.is_valid(&mut conn) {
                            self.inner.error_handler.handle_error(e);
                            internals = self.inner.internals.lock();
                            internals.num_conns -= 1;
                            continue
                        }
                    }

                    return Ok(PooledConnection {
                        pool: self,
                        conn: Some(conn),
                    })
                }
                None => self.inner.cond.wait(&internals),
            }
        }
    }

    fn put_back(&self, mut conn: C) {
        // This is specified to be fast, but call it before locking anyways
        let broken = self.inner.manager.has_broken(&mut conn);

        let mut internals = self.inner.internals.lock();
        if broken {
            internals.num_conns -= 1;
        } else {
            internals.conns.push_back(conn);
            self.inner.cond.notify_one();
        }
    }
}

/// A smart pointer wrapping a connection.
pub struct PooledConnection<'a, C, E, M, H>
        where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    pool: &'a Pool<C, E, M, H>,
    conn: Option<C>,
}

#[unsafe_destructor]
impl<'a, C, E, M, H> Drop for PooledConnection<'a, C, E, M, H>
        where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    fn drop(&mut self) {
        self.pool.put_back(self.conn.take().unwrap());
    }
}

impl<'a, C, E, M, H> Deref<C> for PooledConnection<'a, C, E, M, H>
        where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    fn deref(&self) -> &C {
        self.conn.as_ref().unwrap()
    }
}

impl<'a, C, E, M, H> DerefMut<C> for PooledConnection<'a, C, E, M, H>
        where C: Send, E: Send, M: PoolManager<C, E>, H: ErrorHandler<E> {
    fn deref_mut(&mut self) -> &mut C {
        self.conn.as_mut().unwrap()
    }
}
