use parking_lot::{Mutex, MutexGuard};
use std::cmp;
use std::collections::VecDeque;
use std::error;
use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::task::SpawnExt;
use futures_timer::{Delay, FutureExt as _, Interval};

pub use self::config::{AsyncBuilder, AsyncConfig};
use crate::event::{AcquireEvent, CheckinEvent, CheckoutEvent, ReleaseEvent, TimeoutEvent};
use crate::extensions::Extensions;
use crate::{Error, NopConnectionCustomizer, State, CONNECTION_ID};

mod config;
#[cfg(test)]
mod test;

/// A trait which provides connection-specific functionality.
pub trait AsyncManageConnection: Send + Sync + 'static {
    /// The connection type this manager deals with.
    type Connection: Send + 'static;

    /// The error type returned by `Connection`s.
    type Error: error::Error + Send + 'static;

    /// Attempts to create a new connection.
    fn connect(&self) -> BoxFuture<'_, Result<Self::Connection, Self::Error>>;

    /// Determines if the connection is still connected to the database.
    ///
    /// A standard implementation would check if a simple query like `SELECT 1`
    /// succeeds.
    fn is_valid<'a>(
        &'a self,
        conn: &'a mut Self::Connection,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

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

/// A trait which allows for customization of connections.
pub trait AsyncCustomizeConnection<C, E>: fmt::Debug + Send + Sync + 'static
where
    E: Send + 'static,
{
    /// Called with connections immediately after they are returned from
    /// `ManageConnection::connect`.
    ///
    /// The default implementation simply returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// If this method returns an error, the connection will be discarded.
    #[allow(unused_variables)]
    fn on_acquire<'a>(&'a self, conn: &'a mut C) -> BoxFuture<'a, Result<(), E>> {
        async move { Ok(()) }.boxed()
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

impl<C, E> AsyncCustomizeConnection<C, E> for NopConnectionCustomizer where E: Send + 'static {}

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

struct PoolInternals<M>
where
    M: AsyncManageConnection,
{
    conns: Vec<IdleConn<M::Connection>>,
    num_conns: u32,
    pending_conns: u32,
    last_error: Option<String>,
    waiters: VecDeque<oneshot::Sender<CancellableConn<M>>>,
}

struct SharedPool<M>
where
    M: AsyncManageConnection,
{
    config: AsyncConfig<M::Connection, M::Error>,
    manager: M,
    internals: Mutex<PoolInternals<M>>,
}

fn put_conn<M>(
    shared: &Arc<SharedPool<M>>,
    internals: MutexGuard<PoolInternals<M>>,
    mut conn: IdleConn<M::Connection>,
) where
    M: AsyncManageConnection,
{
    let mut internals = internals;
    loop {
        let send = if let Some(send) = internals.waiters.pop_front() {
            send
        } else {
            internals.conns.push(conn);
            return;
        };
        drop(internals);

        let res = send.send(CancellableConn {
            conn: Some(conn),
            shared: shared.clone(),
        });
        conn = if let Err(conn) = res {
            conn.into_inner()
        } else {
            return;
        };

        internals = shared.internals.lock();
    }
}

fn wait_conn<M>(
    mut internals: MutexGuard<PoolInternals<M>>,
    end: Instant,
) -> impl Future<Output = Result<CancellableConn<M>, std::io::Error>> + Send
where
    M: AsyncManageConnection,
{
    let (send, recv) = oneshot::channel();
    internals.waiters.push_back(send);
    drop(internals);
    async move {
        recv.map_err(|_| -> std::io::Error { panic!("cancel must not happen") })
            .timeout_at(end)
            .await
    }
}

fn drop_conns<M>(
    shared: &Arc<SharedPool<M>>,
    mut internals: MutexGuard<PoolInternals<M>>,
    conns: Vec<Conn<M::Connection>>,
) where
    M: AsyncManageConnection,
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

fn establish_idle_connections<M>(shared: &Arc<SharedPool<M>>, internals: &mut PoolInternals<M>)
where
    M: AsyncManageConnection,
{
    let min = shared.config.min_idle.unwrap_or(shared.config.max_size);
    let idle = internals.conns.len() as u32;
    for _ in idle..min {
        add_connection(shared, internals);
    }
}

fn add_connection<M>(shared: &Arc<SharedPool<M>>, internals: &mut PoolInternals<M>)
where
    M: AsyncManageConnection,
{
    if internals.num_conns + internals.pending_conns >= shared.config.max_size {
        return;
    }

    internals.pending_conns += 1;
    inner(Duration::from_secs(0), shared);

    fn inner<M>(delay: Duration, shared: &Arc<SharedPool<M>>)
    where
        M: AsyncManageConnection,
    {
        let new_shared = Arc::downgrade(shared);
        let timer = shared.config.timer.clone().unwrap_or_else(Default::default);
        let at = Instant::now() + delay;
        SpawnExt::spawn(&mut (&*shared.config.spawn), async move {
            Delay::new_handle(at, timer).await.expect("timer failed");
            let shared = match new_shared.upgrade() {
                Some(shared) => shared,
                None => return,
            };

            let mut conn = shared.manager.connect().await;
            if let Ok(conn_) = &mut conn {
                let res = shared.config.connection_customizer.on_acquire(conn_).await;
                if let Err(e) = res {
                    conn = Err(e);
                }
            }
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
                    internals.pending_conns -= 1;
                    internals.num_conns += 1;
                    put_conn(&shared, internals, conn);
                }
                Err(err) => {
                    shared.internals.lock().last_error = Some(err.to_string());
                    shared.config.error_handler.handle_error(err);
                    let delay = cmp::max(Duration::from_millis(200), delay);
                    let delay = cmp::min(shared.config.connection_timeout / 2, delay * 2);
                    inner(delay, &shared);
                }
            }
        })
        .expect("Failed to spawn");
    }
}

fn reap_connections<M>(shared: &Weak<SharedPool<M>>)
where
    M: AsyncManageConnection,
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
pub struct AsyncPool<M>(Arc<SharedPool<M>>)
where
    M: AsyncManageConnection;

/// Returns a new `Pool` referencing the same state as `self`.
impl<M> Clone for AsyncPool<M>
where
    M: AsyncManageConnection,
{
    fn clone(&self) -> AsyncPool<M> {
        AsyncPool(self.0.clone())
    }
}

impl<M> fmt::Debug for AsyncPool<M>
where
    M: AsyncManageConnection + fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("AsyncPool")
            .field("state", &self.state())
            .field("config", &self.0.config)
            .field("manager", &self.0.manager)
            .finish()
    }
}

impl<M> AsyncPool<M>
where
    M: AsyncManageConnection,
{
    /// Creates a new connection pool with a default configuration.
    pub fn new(manager: M) -> impl Future<Output = Result<AsyncPool<M>, Error>> + Send {
        async move { AsyncPool::builder().build(manager).await }
    }

    /// Returns a builder type to configure a new pool.
    pub fn builder() -> AsyncBuilder<M> {
        AsyncBuilder::new()
    }

    // for testing
    fn new_inner(
        config: AsyncConfig<M::Connection, M::Error>,
        manager: M,
        reaper_rate: Duration,
    ) -> AsyncPool<M> {
        let internals = PoolInternals {
            conns: Vec::with_capacity(config.max_size as usize),
            num_conns: 0,
            pending_conns: 0,
            last_error: None,
            waiters: VecDeque::new(),
        };

        let shared = Arc::new(SharedPool {
            config,
            manager,
            internals: Mutex::new(internals),
        });

        establish_idle_connections(&shared, &mut shared.internals.lock());

        if shared.config.max_lifetime.is_some() || shared.config.idle_timeout.is_some() {
            let s = Arc::downgrade(&shared);
            SpawnExt::spawn(&mut (&*shared.config.spawn), async move {
                let mut interval = Interval::new(reaper_rate);
                while let Some(_) = interval.next().await {
                    reap_connections(&s);
                }
            })
            .expect("spawn failed");
        }

        AsyncPool(shared)
    }

    async fn wait_for_initialization(&self) -> Result<(), Error> {
        let end = Instant::now() + self.0.config.connection_timeout;

        let initial_size = self.0.config.min_idle.unwrap_or(self.0.config.max_size);

        loop {
            let fut = {
                let internals = self.0.internals.lock();
                if internals.num_conns == initial_size {
                    break;
                }
                wait_conn(internals, end)
            };
            if let Err(_) = fut.await {
                let mut internals = self.0.internals.lock();
                return Err(Error(internals.last_error.take()));
            }
        }

        Ok(())
    }

    /// Retrieves a connection from the pool.
    ///
    /// Waits for at most the configured connection timeout before returning an
    /// error.
    pub fn get(&self) -> impl Future<Output = Result<AsyncPooledConnection<M>, Error>> + Send + '_ {
        async move { self.get_timeout(self.0.config.connection_timeout).await }
    }

    /// Retrieves a connection from the pool, waiting for at most `timeout`
    ///
    /// The given timeout will be used instead of the configured connection
    /// timeout.
    pub fn get_timeout(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<AsyncPooledConnection<M>, Error>> + Send + '_ {
        async move {
            let start = Instant::now();
            let end = start + timeout;
            loop {
                let fut = {
                    let mut internals = match self.try_get_inner().await {
                        Ok(conn) => {
                            let event = CheckoutEvent {
                                id: conn.conn.as_ref().unwrap().id,
                                duration: start.elapsed(),
                            };
                            self.0.config.event_handler.handle_checkout(event);
                            return Ok(conn);
                        }
                        Err(i) => i,
                    };

                    add_connection(&self.0, &mut internals);

                    wait_conn(internals, end)
                };

                if let Err(_) = fut.await {
                    let mut internals = self.0.internals.lock();
                    let event = TimeoutEvent { timeout };
                    self.0.config.event_handler.handle_timeout(event);

                    return Err(Error(internals.last_error.take()));
                }
            }
        }
    }

    /// Attempts to retrieve a connection from the pool if there is one
    /// available.
    ///
    /// Returns `None` if there are no idle connections available in the pool.
    /// This method will not block waiting to establish a new connection.
    pub fn try_get(&self) -> impl Future<Output = Option<AsyncPooledConnection<M>>> + Send + '_ {
        async move { self.try_get_inner().await.ok() }
    }

    async fn try_get_inner<'a>(
        &'a self,
    ) -> Result<AsyncPooledConnection<M>, MutexGuard<'a, PoolInternals<M>>> {
        loop {
            let mut conn = self.try_get_inner_notest()?;
            if self.0.config.test_on_check_out {
                if let Err(e) = self.0.manager.is_valid(&mut conn.conn).await {
                    let msg = e.to_string();
                    self.0.config.error_handler.handle_error(e);
                    let mut internals = self.0.internals.lock();
                    internals.last_error = Some(msg);
                    drop_conns(&self.0, internals, vec![conn]);
                    continue;
                }
            }

            return Ok(AsyncPooledConnection {
                pool: self.clone(),
                checkout: Instant::now(),
                conn: Some(conn),
            });
        }
    }

    fn try_get_inner_notest(
        &self,
    ) -> Result<Conn<M::Connection>, MutexGuard<'_, PoolInternals<M>>> {
        loop {
            let mut internals = self.0.internals.lock();
            if let Some(conn) = internals.conns.pop() {
                establish_idle_connections(&self.0, &mut internals);
                drop(internals);

                return Ok(conn.conn);
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

        let internals = self.0.internals.lock();
        if broken {
            drop_conns(&self.0, internals, vec![conn]);
        } else {
            let conn = IdleConn {
                conn,
                idle_start: Instant::now(),
            };
            put_conn(&self.0, internals, conn);
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

struct CancellableConn<M>
where
    M: AsyncManageConnection,
{
    shared: Arc<SharedPool<M>>,
    conn: Option<IdleConn<M::Connection>>,
}

impl<M> CancellableConn<M>
where
    M: AsyncManageConnection,
{
    fn into_inner(mut self) -> IdleConn<M::Connection> {
        self.conn.take().unwrap()
    }
}

impl<M> Drop for CancellableConn<M>
where
    M: AsyncManageConnection,
{
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let internals = self.shared.internals.lock();
            put_conn(&self.shared, internals, conn);
        }
    }
}

/// A smart pointer wrapping a connection.
pub struct AsyncPooledConnection<M>
where
    M: AsyncManageConnection,
{
    pool: AsyncPool<M>,
    checkout: Instant,
    conn: Option<Conn<M::Connection>>,
}

impl<M> fmt::Debug for AsyncPooledConnection<M>
where
    M: AsyncManageConnection,
    M::Connection: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.conn.as_ref().unwrap().conn, fmt)
    }
}

impl<M> Drop for AsyncPooledConnection<M>
where
    M: AsyncManageConnection,
{
    fn drop(&mut self) {
        self.pool.put_back(self.checkout, self.conn.take().unwrap());
    }
}

impl<M> Deref for AsyncPooledConnection<M>
where
    M: AsyncManageConnection,
{
    type Target = M::Connection;

    fn deref(&self) -> &M::Connection {
        &self.conn.as_ref().unwrap().conn
    }
}

impl<M> DerefMut for AsyncPooledConnection<M>
where
    M: AsyncManageConnection,
{
    fn deref_mut(&mut self) -> &mut M::Connection {
        &mut self.conn.as_mut().unwrap().conn
    }
}

impl<M> AsyncPooledConnection<M>
where
    M: AsyncManageConnection,
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
