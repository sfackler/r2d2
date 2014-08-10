//! A library providing a generic connection pool.
#![feature(unsafe_destructor)]
#![warn(missing_doc)]

use std::default::Default;
use std::sync::Mutex;
use std::fmt;

/// A trait which provides database-specific functionality.
pub trait PoolManager<C, E> {
    /// Attempts to create a new connection.
    fn connect(&self) -> Result<C, E>;

    /// Determines if the connection is still connected to the database.
    ///
    /// A standard implementation would check if a simple query like `SELECT 1`
    /// succeeds.
    fn is_valid(&self, conn: &C) -> bool;
}

/// A struct specifying the runtime configuration of a pool.
///
/// `Config` implements `Default`, which provides a set of reasonable default
/// values.
pub struct Config {
    /// The number of connections that will be made during pool creation.
    ///
    /// Must be no greater than `max_size`.
    ///
    /// Defaults to 3.
    pub initial_size: uint,
    /// The maximum number of connections that the pool will maintain.
    ///
    /// Must be positive and no less than `initial_size`.
    ///
    /// Defaults to 15.
    pub max_size: uint,
    /// The number of connections that will be created at once when the pool is
    /// exhausted.
    ///
    /// Must be positive.
    ///
    /// Defaults to 3.
    pub acquire_increment: uint,
    /// The number of tasks that the pool will use for asynchronous operations
    /// such as connection creation and health checks.
    ///
    /// Must be positive.
    ///
    /// Defaults to 3.
    pub helper_tasks: uint,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            initial_size: 3,
            max_size: 15,
            acquire_increment: 3,
            helper_tasks: 3,
        }
    }
}

impl Config {
    /// Determines if the configuration is valid
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_size == 0 {
            return Err("max_size must be positive");
        }

        if self.initial_size > self.max_size {
            return Err("initial_size cannot be greater than max_size");
        }

        if self.acquire_increment == 0 {
            return Err("acquire_increment must be positive");
        }

        if self.helper_tasks == 0 {
            return Err("helper_tasks must be positive");
        }

        Ok(())
    }
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
    conns: Vec<C>,
    conn_count: uint,
}

/// A generic connection pool.
pub struct Pool<C, M> {
    config: Config,
    manager: M,
    internals: Mutex<PoolInternals<C>>,
}

impl<C: Send, E, M: PoolManager<C, E>> Pool<C, M> {
    /// Creates a new connection pool.
    ///
    /// `Config::initial_size` connections will be created synchronously before
    /// this method returns.
    pub fn new(config: Config, manager: M) -> Result<Pool<C, M>, NewPoolError<E>> {
        match config.validate() {
            Ok(()) => {}
            Err(err) => return Err(InvalidConfig(err))
        }

        let mut internals = PoolInternals {
            conns: vec![],
            conn_count: config.initial_size,
        };

        for _ in range(0, config.initial_size) {
            match manager.connect() {
                Ok(conn) => internals.conns.push(conn),
                Err(err) => return Err(ConnectionError(err)),
            }
        }

        Ok(Pool {
            config: config,
            manager: manager,
            internals: Mutex::new(internals),
        })
    }

    /// Retrieves a connection from the pool.
    pub fn get<'a>(&'a self) -> Result<PooledConnection<'a, C, M>, E> {
        let mut internals = self.internals.lock();

        loop {
            match internals.conns.pop() {
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
        let mut internals = self.internals.lock();
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
/// or the `PooledConnection`'s destructor `fail!()`.
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
            fail!("You must call conn.return()");
        }
    }
}

impl<'a, C, M> Deref<C> for PooledConnection<'a, C, M> {
    fn deref(&self) -> &C {
        self.conn.get_ref()
    }
}
