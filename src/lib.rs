#![feature(unsafe_destructor)]

use std::default::Default;
use std::sync::Mutex;

pub trait PoolManager<C, E> {
    fn connect(&self) -> Result<C, E>;
}

pub struct Config {
    pub initial_size: uint,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            initial_size: 3,
        }
    }
}

struct PoolInternals<C> {
    conns: Vec<C>,
}

pub struct Pool<C, M> {
    config: Config,
    manager: M,
    internals: Mutex<PoolInternals<C>>,
}

impl<C: Send, E, M: PoolManager<C, E>+Default> Pool<C, M> {
    pub fn new(config: Config) -> Pool<C, M> {
        Pool::with_manager(config, Default::default())
    }
}

impl<C: Send, E, M: PoolManager<C, E>> Pool<C, M> {
    pub fn with_manager(config: Config, manager: M) -> Pool<C, M> {
        Pool {
            config: config,
            manager: manager,
            internals: Mutex::new(PoolInternals {
                conns: vec![]
            }),
        }
    }

    fn put_back(&self, conn: C) {
    }
}

pub struct PooledConnection<'a, C, M> {
    pool: &'a Pool<C, M>,
    conn: Option<C>,
}

#[unsafe_destructor]
impl<'a, C: Send, E, M: PoolManager<C, E>> Drop for PooledConnection<'a, C, M> {
    fn drop(&mut self) {
        self.pool.put_back(self.conn.take_unwrap())
    }
}

impl<'a, C, M> Deref<C> for PooledConnection<'a, C, M> {
    fn deref<'b>(&'b self) -> &'b C {
        self.conn.get_ref()
    }
}
