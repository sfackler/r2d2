#![feature(unsafe_destructor)]

use std::default::Default;
use std::sync::Mutex;
use std::fmt;

pub trait PoolManager<C, E> {
    fn connect(&self) -> Result<C, E>;
}

pub struct Config {
    pub initial_size: uint,
    pub max_size: uint,
    pub acquire_increment: uint,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            initial_size: 3,
            max_size: 15,
            acquire_increment: 3,
        }
    }
}

impl Config {
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

        Ok(())
    }
}

#[deriving(PartialEq, Eq)]
pub enum NewPoolError<E> {
    InvalidConfig(&'static str),
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

pub struct Pool<C, M> {
    config: Config,
    manager: M,
    internals: Mutex<PoolInternals<C>>,
}

impl<C: Send, E, M: PoolManager<C, E>> Pool<C, M> {
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

pub struct PooledConnection<'a, C, M> {
    pool: &'a Pool<C, M>,
    conn: Option<C>,
}

impl<'a, C: Send, E, M: PoolManager<C, E>> PooledConnection<'a, C, M> {
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
