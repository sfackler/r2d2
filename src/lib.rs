//! A library providing a generic connection pool.
#![feature(unsafe_destructor)]
#![warn(missing_doc)]
#![doc(html_root_url="http://www.rust-ci.org/sfackler/r2d2/doc")]

use std::collections::{Deque, RingBuf};
use std::sync::{Arc, Mutex};
use std::fmt;

pub use config::Config;

mod config;

/// A trait which provides database-specific functionality.
pub trait PoolManager<C, E>: Send+Sync {
    /// Attempts to create a new connection.
    fn connect(&self) -> Result<C, E>;

    /// Determines if the connection is still connected to the database.
    ///
    /// A standard implementation would check if a simple query like `SELECT 1`
    /// succeeds.
    fn is_valid(&self, conn: &C) -> bool;
}

/// An error type returned if pool creation fails.
#[deriving(PartialEq, Eq)]
pub enum NewPoolError<E> {
    /// The provided pool configuration was invalid.
    InvalidConfig(&'static str),
    /// The manager returned an error when creating a connection.
    ConnectionError(E),
}

impl<E: fmt::Show> fmt::Show for NewPoolError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            InvalidConfig(ref error) => write!(f, "Invalid config: {}", error),
            ConnectionError(ref error) => write!(f, "Unable to create connections: {}", error),
        }
    }
}

struct PoolInternals<C> {
    conns: RingBuf<C>,
    conn_count: uint,
}

struct InnerPool<C, M> {
    config: Config,
    manager: M,
    internals: Mutex<PoolInternals<C>>,
}

/// A generic connection pool.
pub struct Pool<C, M> {
    inner: Arc<InnerPool<C, M>>
}

impl<C: Send, E, M: PoolManager<C, E>> Pool<C, M> {
    /// Creates a new connection pool.
    pub fn new(config: Config, manager: M) -> Result<Pool<C, M>, NewPoolError<E>> {
        match config.validate() {
            Ok(()) => {}
            Err(err) => return Err(InvalidConfig(err))
        }

        let mut internals = PoolInternals {
            conns: RingBuf::new(),
            conn_count: config.initial_size,
        };

        for _ in range(0, config.initial_size) {
            match manager.connect() {
                Ok(conn) => internals.conns.push(conn),
                Err(err) => return Err(ConnectionError(err)),
            }
        }

        let inner = InnerPool {
            config: config,
            manager: manager,
            internals: Mutex::new(internals),
        };

        Ok(Pool {
            inner: Arc::new(inner),
        })
    }

    /// Retrieves a connection from the pool.
    pub fn get<'a>(&'a self) -> Result<PooledConnection<'a, C, M>, E> {
        let mut internals = self.inner.internals.lock();

        loop {
            match internals.conns.pop_front() {
                Some(conn) => {
                    return Ok(PooledConnection {
                        pool: self,
                        conn: Some(conn)
                    })
                }
                None => internals.cond.wait(),
            }
        }
    }

    fn put_back(&self, conn: C) {
        let mut internals = self.inner.internals.lock();
        internals.conns.push(conn);
        internals.cond.signal();
    }
}

/// A smart pointer wrapping an underlying connection.
///
/// ## Note
///
/// Due to Rust bug [#15905](https://github.com/rust-lang/rust/issues/15905),
/// the connection cannot be automatically returned to its pool when the
/// `PooledConnection` drops out of scope. The `replace` method must be called,
/// or the `PooledConnection`'s destructor will `fail!()`.
pub struct PooledConnection<'a, C, M> {
    pool: &'a Pool<C, M>,
    conn: Option<C>,
}

impl<'a, C: Send, E, M: PoolManager<C, E>> PooledConnection<'a, C, M> {
    /// Consumes the `PooledConnection`, returning the connection to its pool.
    ///
    /// This must be called before the `PooledConnection` drops out of scope or
    /// its destructor will `fail!()`.
    pub fn replace(mut self) {
        self.pool.put_back(self.conn.take_unwrap())
    }
}

#[unsafe_destructor]
impl<'a, C, M> Drop for PooledConnection<'a, C, M> {
    fn drop(&mut self) {
        if self.conn.is_some() {
            fail!("You must call conn.replace()");
        }
    }
}

impl<'a, C, M> Deref<C> for PooledConnection<'a, C, M> {
    fn deref(&self) -> &C {
        self.conn.get_ref()
    }
}
