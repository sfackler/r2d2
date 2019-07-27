use futures::executor::ThreadPool;
use futures::future::FutureObj;
use futures::prelude::*;
use futures::task::{Spawn, SpawnError};
use futures_timer::TimerHandle;
use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

use super::{AsyncCustomizeConnection, AsyncManageConnection, AsyncPool};
use crate::event::HandleEvent;
use crate::{Error, HandleError, LoggingErrorHandler, NopConnectionCustomizer, NopEventHandler};

/// A builder for a connection pool.
pub struct AsyncBuilder<M>
where
    M: AsyncManageConnection,
{
    max_size: u32,
    min_idle: Option<u32>,
    test_on_check_out: bool,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
    connection_timeout: Duration,
    error_handler: Box<dyn HandleError<M::Error>>,
    connection_customizer: Box<dyn AsyncCustomizeConnection<M::Connection, M::Error>>,
    event_handler: Box<dyn HandleEvent>,
    spawn: Option<Box<dyn SharedSpawn + Send + Sync>>,
    timer: Option<TimerHandle>,
    reaper_rate: Duration,
    _p: PhantomData<M>,
}

impl<M> fmt::Debug for AsyncBuilder<M>
where
    M: AsyncManageConnection,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("AsyncBuilder")
            .field("max_size", &self.max_size)
            .field("min_idle", &self.min_idle)
            .field("test_on_check_out", &self.test_on_check_out)
            .field("max_lifetime", &self.max_lifetime)
            .field("idle_timeout", &self.idle_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .field("error_handler", &self.error_handler)
            .field("event_handler", &self.event_handler)
            .field("connection_customizer", &self.connection_customizer)
            .finish()
    }
}

impl<M> Default for AsyncBuilder<M>
where
    M: AsyncManageConnection,
{
    fn default() -> AsyncBuilder<M> {
        AsyncBuilder {
            max_size: 10,
            min_idle: None,
            test_on_check_out: true,
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            connection_timeout: Duration::from_secs(30),
            error_handler: Box::new(LoggingErrorHandler),
            event_handler: Box::new(NopEventHandler),
            connection_customizer: Box::new(NopConnectionCustomizer),
            spawn: None,
            timer: None,
            reaper_rate: Duration::from_secs(30),
            _p: PhantomData,
        }
    }
}

impl<M> AsyncBuilder<M>
where
    M: AsyncManageConnection,
{
    /// Constructs a new `AsyncBuilder`.
    ///
    /// Parameters are initialized with their default values.
    pub fn new() -> AsyncBuilder<M> {
        AsyncBuilder::default()
    }

    /// Sets the maximum number of connections managed by the pool.
    ///
    /// Defaults to 10.
    ///
    /// # Panics
    ///
    /// Panics if `max_size` is 0.
    pub fn max_size(mut self, max_size: u32) -> AsyncBuilder<M> {
        assert!(max_size > 0, "max_size must be positive");
        self.max_size = max_size;
        self
    }

    /// Sets the minimum idle connection count maintained by the pool.
    ///
    /// If set, the pool will try to maintain at least this many idle
    /// connections at all times, while respecting the value of `max_size`.
    ///
    /// Defaults to `None` (equivalent to the value of `max_size`).
    pub fn min_idle(mut self, min_idle: Option<u32>) -> AsyncBuilder<M> {
        self.min_idle = min_idle;
        self
    }

    /// Sets the timer object used for asynchronous operations such as connection
    /// creation.
    ///
    /// Defaults to a new pool with 3 threads.
    pub fn spawn(mut self, spawn: Box<dyn SharedSpawn + Send + Sync>) -> AsyncBuilder<M> {
        self.spawn = Some(spawn);
        self
    }

    /// Sets the timer object used for asynchronous operations such as connection
    /// creation.
    ///
    /// Defaults to the global timer.
    pub fn timer(mut self, timer: TimerHandle) -> AsyncBuilder<M> {
        self.timer = Some(timer);
        self
    }

    /// If true, the health of a connection will be verified via a call to
    /// `ConnectionManager::is_valid` before it is checked out of the pool.
    ///
    /// Defaults to true.
    pub fn test_on_check_out(mut self, test_on_check_out: bool) -> AsyncBuilder<M> {
        self.test_on_check_out = test_on_check_out;
        self
    }

    /// Sets the maximum lifetime of connections in the pool.
    ///
    /// If set, connections will be closed after existing for at most 30 seconds
    /// beyond this duration.
    ///
    /// If a connection reaches its maximum lifetime while checked out it will
    /// be closed when it is returned to the pool.
    ///
    /// Defaults to 30 minutes.
    ///
    /// # Panics
    ///
    /// Panics if `max_lifetime` is the zero `Duration`.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> AsyncBuilder<M> {
        assert_ne!(max_lifetime, Some(Duration::from_secs(0)), "max_lifetime must be positive");
        self.max_lifetime = max_lifetime;
        self
    }

    /// Sets the idle timeout used by the pool.
    ///
    /// If set, connections will be closed after sitting idle for at most 30
    /// seconds beyond this duration.
    ///
    /// Defaults to 10 minutes.
    ///
    /// # Panics
    ///
    /// Panics if `idle_timeout` is the zero `Duration`.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> AsyncBuilder<M> {
        assert_ne!(idle_timeout, Some(Duration::from_secs(0)), "idle_timeout must be positive");
        self.idle_timeout = idle_timeout;
        self
    }

    /// Sets the connection timeout used by the pool.
    ///
    /// Calls to `Pool::get` will wait this long for a connection to become
    /// available before returning an error.
    ///
    /// Defaults to 30 seconds.
    ///
    /// # Panics
    ///
    /// Panics if `connection_timeout` is the zero duration
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> AsyncBuilder<M> {
        assert!(
            connection_timeout > Duration::from_secs(0),
            "connection_timeout must be positive"
        );
        self.connection_timeout = connection_timeout;
        self
    }

    /// Sets the handler for errors reported in the pool.
    ///
    /// Defaults to the `LoggingErrorHandler`.
    pub fn error_handler(
        mut self,
        error_handler: Box<dyn HandleError<M::Error>>,
    ) -> AsyncBuilder<M> {
        self.error_handler = error_handler;
        self
    }

    /// Sets the handler for events reported by the pool.
    ///
    /// Defaults to the `NopEventHandler`.
    pub fn event_handler(mut self, event_handler: Box<dyn HandleEvent>) -> AsyncBuilder<M> {
        self.event_handler = event_handler;
        self
    }

    /// Sets the connection customizer used by the pool.
    ///
    /// Defaults to the `NopConnectionCustomizer`.
    pub fn connection_customizer(
        mut self,
        connection_customizer: Box<dyn AsyncCustomizeConnection<M::Connection, M::Error>>,
    ) -> AsyncBuilder<M> {
        self.connection_customizer = connection_customizer;
        self
    }

    // used by tests
    #[allow(dead_code)]
    pub(crate) fn reaper_rate(mut self, reaper_rate: Duration) -> AsyncBuilder<M> {
        self.reaper_rate = reaper_rate;
        self
    }

    /// Consumes the builder, returning a new, initialized pool.
    ///
    /// It will block until the pool has established its configured minimum
    /// number of connections, or it times out.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool is unable to open its minimum number of
    /// connections.
    ///
    /// # Panics
    ///
    /// Panics if `min_idle` is greater than `max_size`.
    pub fn build(self, manager: M) -> impl Future<Output = Result<AsyncPool<M>, Error>> + Send {
        async move {
            let pool = self.build_unchecked(manager);
            pool.wait_for_initialization().await?;
            Ok(pool)
        }
    }

    /// Consumes the builder, returning a new pool.
    ///
    /// Unlike `build`, this method does not wait for any connections to be
    /// established before returning.
    ///
    /// # Panics
    ///
    /// Panics if `min_idle` is greater than `max_size`.
    pub fn build_unchecked(self, manager: M) -> AsyncPool<M> {
        if let Some(min_idle) = self.min_idle {
            assert!(
                self.max_size >= min_idle,
                "min_idle must be no larger than max_size"
            );
        }

        let spawn = if let Some(spawn) = self.spawn {
            spawn
        } else {
            let tp = ThreadPool::builder()
                .pool_size(3)
                .create()
                .expect("Could not create thread pool");
            Box::new(tp)
        };

        let config = AsyncConfig {
            max_size: self.max_size,
            min_idle: self.min_idle,
            test_on_check_out: self.test_on_check_out,
            max_lifetime: self.max_lifetime,
            idle_timeout: self.idle_timeout,
            connection_timeout: self.connection_timeout,
            error_handler: self.error_handler,
            event_handler: self.event_handler,
            connection_customizer: self.connection_customizer,
            spawn: spawn,
            timer: self.timer,
        };

        AsyncPool::new_inner(config, manager, self.reaper_rate)
    }
}

pub struct AsyncConfig<C, E> {
    pub max_size: u32,
    pub min_idle: Option<u32>,
    pub test_on_check_out: bool,
    pub max_lifetime: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub connection_timeout: Duration,
    pub error_handler: Box<dyn HandleError<E>>,
    pub event_handler: Box<dyn HandleEvent>,
    pub connection_customizer: Box<dyn AsyncCustomizeConnection<C, E>>,
    pub spawn: Box<dyn SharedSpawn + Send + Sync>,
    pub timer: Option<TimerHandle>,
}

// manual to avoid bounds on C and E
impl<C, E> fmt::Debug for AsyncConfig<C, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Config")
            .field("max_size", &self.max_size)
            .field("min_idle", &self.min_idle)
            .field("test_on_check_out", &self.test_on_check_out)
            .field("max_lifetime", &self.max_lifetime)
            .field("idle_timeout", &self.idle_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .field("error_handler", &self.error_handler)
            .field("event_handler", &self.event_handler)
            .field("connection_customizer", &self.connection_customizer)
            .field("timer", &self.timer)
            .finish()
    }
}

/// Essentially an alias of `&Spawn`.
pub trait SharedSpawn {
    fn spawn_obj(&self, future: FutureObj<'static, ()>) -> Result<(), SpawnError>;
}

impl<Sp> SharedSpawn for Sp
where
    Sp: ?Sized,
    for<'a> &'a Sp: Spawn,
{
    fn spawn_obj(mut self: &Self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        Spawn::spawn_obj(&mut self, future)
    }
}

impl<'a> Spawn for dyn SharedSpawn + 'a {
    fn spawn_obj(&mut self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        <Self as SharedSpawn>::spawn_obj(self, future)
    }
}
impl<'a> Spawn for &(dyn SharedSpawn + 'a) {
    fn spawn_obj(&mut self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        <dyn SharedSpawn as SharedSpawn>::spawn_obj(*self, future)
    }
}
impl<'a> Spawn for dyn SharedSpawn + Send + 'a {
    fn spawn_obj(&mut self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        <Self as SharedSpawn>::spawn_obj(self, future)
    }
}
impl<'a> Spawn for &(dyn SharedSpawn + Send + 'a) {
    fn spawn_obj(&mut self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        <dyn SharedSpawn as SharedSpawn>::spawn_obj(*self, future)
    }
}
impl<'a> Spawn for dyn SharedSpawn + Send + Sync + 'a {
    fn spawn_obj(&mut self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        <Self as SharedSpawn>::spawn_obj(self, future)
    }
}
impl<'a> Spawn for &(dyn SharedSpawn + Send + Sync + 'a) {
    fn spawn_obj(&mut self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        <dyn SharedSpawn as SharedSpawn>::spawn_obj(*self, future)
    }
}
