use futures::future::BoxFuture;
use futures::prelude::*;
use futures_timer::Delay;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{error, fmt, mem};

use super::{AsyncCustomizeConnection, AsyncManageConnection, AsyncPool, AsyncPooledConnection};
use crate::event::{AcquireEvent, CheckinEvent, CheckoutEvent, ReleaseEvent, TimeoutEvent};
use crate::HandleEvent;

#[derive(Debug)]
pub struct Error;

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str("blammo")
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        "Error"
    }
}

#[derive(Debug, PartialEq)]
struct FakeConnection(bool);

struct OkManager;

impl AsyncManageConnection for OkManager {
    type Connection = FakeConnection;
    type Error = Error;

    fn connect(&self) -> BoxFuture<'_, Result<FakeConnection, Error>> {
        async move { Ok(FakeConnection(true)) }.boxed()
    }

    fn is_valid<'a>(&'a self, _: &'a mut FakeConnection) -> BoxFuture<'a, Result<(), Error>> {
        async move { Ok(()) }.boxed()
    }

    fn has_broken(&self, _: &mut FakeConnection) -> bool {
        false
    }
}

struct NthConnectFailManager {
    n: Mutex<u32>,
}

impl AsyncManageConnection for NthConnectFailManager {
    type Connection = FakeConnection;
    type Error = Error;

    fn connect(&self) -> BoxFuture<'_, Result<FakeConnection, Error>> {
        async move {
            let mut n = self.n.lock();
            if *n > 0 {
                *n -= 1;
                Ok(FakeConnection(true))
            } else {
                Err(Error)
            }
        }
            .boxed()
    }

    fn is_valid<'a>(&'a self, _: &'a mut FakeConnection) -> BoxFuture<'a, Result<(), Error>> {
        async move { Ok(()) }.boxed()
    }

    fn has_broken(&self, _: &mut FakeConnection) -> bool {
        false
    }
}

#[runtime::test]
async fn test_max_size_ok() {
    let manager = NthConnectFailManager { n: Mutex::new(5) };
    let pool = AsyncPool::builder()
        .max_size(5)
        .build(manager)
        .await
        .unwrap();
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().await.ok().unwrap());
    }
}

#[runtime::test]
async fn test_acquire_release() {
    let pool = AsyncPool::builder()
        .max_size(2)
        .build(OkManager)
        .await
        .unwrap();

    let conn1 = pool.get().await.ok().unwrap();
    let conn2 = pool.get().await.ok().unwrap();
    drop(conn1);
    let conn3 = pool.get().await.ok().unwrap();
    drop(conn2);
    drop(conn3);
}

#[runtime::test]
async fn try_get() {
    let pool = AsyncPool::builder()
        .max_size(2)
        .build(OkManager)
        .await
        .unwrap();

    let conn1 = pool.try_get().await;
    let conn2 = pool.try_get().await;
    let conn3 = pool.try_get().await;

    assert!(conn1.is_some());
    assert!(conn2.is_some());
    assert!(conn3.is_none());

    drop(conn1);

    assert!(pool.try_get().await.is_some());
}

#[runtime::test]
async fn get_timeout() {
    let pool = AsyncPool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(500))
        .build(OkManager)
        .await
        .unwrap();

    let timeout = Duration::from_millis(100);
    let succeeds_immediately = pool.get_timeout(timeout).await;

    assert!(succeeds_immediately.is_ok());

    let t1 = runtime::spawn(async move {
        Delay::new(Duration::from_millis(50)).await.unwrap();
        drop(succeeds_immediately);
    });

    let succeeds_delayed = pool.get_timeout(timeout).await;
    assert!(succeeds_delayed.is_ok());

    let t2 = runtime::spawn(async move {
        Delay::new(Duration::from_millis(150)).await.unwrap();
        drop(succeeds_delayed);
    });

    let fails = pool.get_timeout(timeout).await;
    assert!(fails.is_err());

    t1.await;
    t2.await;
}

#[test]
fn test_is_send_sync() {
    fn is_send_sync<T: Send + Sync>() {}
    is_send_sync::<AsyncPool<OkManager>>();
}

#[runtime::test]
async fn test_issue_2_unlocked_during_is_valid() {
    struct BlockingChecker {
        first: AtomicBool,
        s: Mutex<SyncSender<()>>,
        r: Mutex<Receiver<()>>,
    }

    impl AsyncManageConnection for BlockingChecker {
        type Connection = FakeConnection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<FakeConnection, Error>> {
            async move { Ok(FakeConnection(true)) }.boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut FakeConnection) -> BoxFuture<'a, Result<(), Error>> {
            async move {
                if self.first.compare_and_swap(true, false, Ordering::SeqCst) {
                    self.s.lock().send(()).unwrap();
                    self.r.lock().recv().unwrap();
                }
                Ok(())
            }
                .boxed()
        }

        fn has_broken(&self, _: &mut FakeConnection) -> bool {
            false
        }
    }

    let (s1, r1) = mpsc::sync_channel(0);
    let (s2, r2) = mpsc::sync_channel(0);

    let manager = BlockingChecker {
        first: AtomicBool::new(true),
        s: Mutex::new(s1),
        r: Mutex::new(r2),
    };

    let pool = AsyncPool::builder()
        .test_on_check_out(true)
        .max_size(2)
        .build(manager)
        .await
        .unwrap();

    let p2 = pool.clone();
    let t = runtime::spawn(async move {
        p2.get().await.ok().unwrap();
    });

    r1.recv().unwrap();
    // get call by other task has triggered the health check
    pool.get().await.ok().unwrap();
    s2.send(()).ok().unwrap();

    t.await;
}

#[runtime::test]
async fn test_drop_on_broken() {
    static DROPPED: AtomicBool = AtomicBool::new(false);
    DROPPED.store(false, Ordering::SeqCst);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }

    struct Handler;

    impl AsyncManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<Connection, Error>> {
            async move { Ok(Connection) }.boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            true
        }
    }

    let pool = AsyncPool::new(Handler).await.unwrap();

    drop(pool.get().await.ok().unwrap());

    assert!(DROPPED.load(Ordering::SeqCst));
}

#[runtime::test]
async fn test_initialization_failure() {
    let manager = NthConnectFailManager { n: Mutex::new(0) };
    let err = AsyncPool::builder()
        .connection_timeout(Duration::from_secs(1))
        .build(manager)
        .await
        .err()
        .unwrap();
    assert!(err.to_string().contains("blammo"));
}

#[runtime::test]
async fn test_lazy_initialization_failure() {
    let manager = NthConnectFailManager { n: Mutex::new(0) };
    let pool = AsyncPool::builder()
        .connection_timeout(Duration::from_secs(1))
        .build_unchecked(manager);
    let err = pool.get().await.err().unwrap();
    assert!(err.to_string().contains("blammo"));
}

#[runtime::test]
async fn test_get_global_timeout() {
    let pool = AsyncPool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_secs(1))
        .build(OkManager)
        .await
        .unwrap();
    let _c = pool.get().await.unwrap();
    let started_waiting = Instant::now();
    pool.get().await.err().unwrap();
    // Elapsed time won't be *exactly* 1 second, but it will certainly be
    // less than 2 seconds
    assert_eq!(started_waiting.elapsed().as_secs(), 1);
}

#[runtime::test]
async fn test_connection_customizer() {
    static RELEASED: AtomicBool = AtomicBool::new(false);
    RELEASED.store(false, Ordering::SeqCst);

    static DROPPED: AtomicBool = AtomicBool::new(false);
    DROPPED.store(false, Ordering::SeqCst);

    struct Connection(i32);

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }

    struct Handler;

    impl AsyncManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<Connection, Error>> {
            async move { Ok(Connection(0)) }.boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct Customizer;

    impl AsyncCustomizeConnection<Connection, Error> for Customizer {
        fn on_acquire<'a>(&'a self, conn: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move {
                if !DROPPED.load(Ordering::SeqCst) {
                    Err(Error)
                } else {
                    conn.0 = 1;
                    Ok(())
                }
            }
                .boxed()
        }

        fn on_release(&self, _: Connection) {
            RELEASED.store(true, Ordering::SeqCst);
        }
    }

    let pool = AsyncPool::builder()
        .connection_customizer(Box::new(Customizer))
        .build(Handler)
        .await
        .unwrap();

    {
        let conn = pool.get().await.unwrap();
        assert_eq!(1, conn.0);
        assert!(!RELEASED.load(Ordering::SeqCst));
        assert!(DROPPED.load(Ordering::SeqCst));
    }
    assert!(RELEASED.load(Ordering::SeqCst));
}

#[runtime::test]
async fn test_idle_timeout() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler(AtomicIsize);

    impl AsyncManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<Connection, Error>> {
            async move {
                if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                    Ok(Connection)
                } else {
                    Err(Error)
                }
            }
                .boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = AsyncPool::builder()
        .max_size(5)
        .idle_timeout(Some(Duration::from_secs(1)))
        .reaper_rate(Duration::from_secs(1))
        .build(Handler(AtomicIsize::new(5)))
        .await
        .unwrap();
    let conn = pool.get().await.unwrap();
    Delay::new(Duration::from_secs(2)).await.unwrap();
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    drop(conn);
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
}

#[runtime::test]
async fn idle_timeout_partial_use() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler(AtomicIsize);

    impl AsyncManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<Connection, Error>> {
            async move {
                if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                    Ok(Connection)
                } else {
                    Err(Error)
                }
            }
                .boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = AsyncPool::builder()
        .max_size(5)
        .idle_timeout(Some(Duration::from_secs(1)))
        .reaper_rate(Duration::from_secs(1))
        .build(Handler(AtomicIsize::new(5)))
        .await
        .unwrap();
    for _ in 0..8 {
        Delay::new(Duration::from_millis(250)).await.unwrap();
        pool.get().await.unwrap();
    }
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    assert_eq!(1, pool.state().connections);
}

#[runtime::test]
async fn test_max_lifetime() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler(AtomicIsize);

    impl AsyncManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<Connection, Error>> {
            async move {
                if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                    Ok(Connection)
                } else {
                    Err(Error)
                }
            }
                .boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = AsyncPool::builder()
        .max_size(5)
        .max_lifetime(Some(Duration::from_secs(1)))
        .connection_timeout(Duration::from_secs(1))
        .reaper_rate(Duration::from_secs(1))
        .build(Handler(AtomicIsize::new(5)))
        .await
        .unwrap();
    let conn = pool.get().await.unwrap();
    Delay::new(Duration::from_secs(2)).await.unwrap();
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    drop(conn);
    Delay::new(Duration::from_secs(2)).await.unwrap();
    assert_eq!(5, DROPPED.load(Ordering::SeqCst));
    assert!(pool.get().await.is_err());
}

#[runtime::test]
async fn min_idle() {
    struct Connection;

    struct Handler;

    impl AsyncManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<Connection, Error>> {
            async move { Ok(Connection) }.boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = AsyncPool::builder()
        .max_size(5)
        .min_idle(Some(2))
        .build(Handler)
        .await
        .unwrap();
    Delay::new(Duration::from_secs(1)).await.unwrap();
    assert_eq!(2, pool.state().idle_connections);
    assert_eq!(2, pool.state().connections);
    let conns = stream::iter(0..3)
        .then(|_| async { pool.get().await.unwrap() })
        .collect::<Vec<_>>()
        .await;
    Delay::new(Duration::from_secs(1)).await.unwrap();
    assert_eq!(2, pool.state().idle_connections);
    assert_eq!(5, pool.state().connections);
    mem::drop(conns);
    assert_eq!(5, pool.state().idle_connections);
    assert_eq!(5, pool.state().connections);
}

#[runtime::test]
async fn conns_drop_on_pool_drop() {
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler;

    impl AsyncManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<Connection, Error>> {
            async move { Ok(Connection) }.boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut Connection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = AsyncPool::builder()
        .max_lifetime(Some(Duration::from_secs(10)))
        .max_size(10)
        .build(Handler)
        .await
        .unwrap();
    drop(pool);
    for _ in 0..10 {
        if DROPPED.load(Ordering::SeqCst) == 10 {
            return;
        }
        Delay::new(Duration::from_secs(1)).await.unwrap();
    }
    panic!("timed out waiting for connections to drop");
}

#[runtime::test]
async fn events() {
    #[derive(Debug)]
    enum Event {
        Acquire(AcquireEvent),
        Release(ReleaseEvent),
        Checkout(CheckoutEvent),
        Checkin(CheckinEvent),
        Timeout(TimeoutEvent),
    }

    #[derive(Debug)]
    struct TestEventHandler(Arc<Mutex<Vec<Event>>>);

    impl HandleEvent for TestEventHandler {
        fn handle_acquire(&self, event: AcquireEvent) {
            self.0.lock().push(Event::Acquire(event));
        }

        fn handle_release(&self, event: ReleaseEvent) {
            self.0.lock().push(Event::Release(event));
        }

        fn handle_checkout(&self, event: CheckoutEvent) {
            self.0.lock().push(Event::Checkout(event));
        }

        fn handle_timeout(&self, event: TimeoutEvent) {
            self.0.lock().push(Event::Timeout(event));
        }

        fn handle_checkin(&self, event: CheckinEvent) {
            self.0.lock().push(Event::Checkin(event));
        }
    }

    struct TestConnection;

    struct TestConnectionManager;

    impl AsyncManageConnection for TestConnectionManager {
        type Connection = TestConnection;
        type Error = Error;

        fn connect(&self) -> BoxFuture<'_, Result<TestConnection, Error>> {
            async move {
                // Avoid acquisition-before-release flakiness
                Delay::new(Duration::from_millis(10)).await.unwrap();
                Ok(TestConnection)
            }
                .boxed()
        }

        fn is_valid<'a>(&'a self, _: &'a mut TestConnection) -> BoxFuture<'a, Result<(), Error>> {
            async move { Ok(()) }.boxed()
        }

        fn has_broken(&self, _: &mut TestConnection) -> bool {
            true
        }
    }

    let events = Arc::new(Mutex::new(vec![]));

    let creation = Instant::now();
    let pool = AsyncPool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(250))
        .event_handler(Box::new(TestEventHandler(events.clone())))
        .build(TestConnectionManager)
        .await
        .unwrap();

    let start = Instant::now();
    let conn = pool.get().await.unwrap();
    let checkout = start.elapsed();

    pool.get_timeout(Duration::from_millis(123))
        .await
        .err()
        .unwrap();

    drop(conn);
    let checkin = start.elapsed();
    let release = creation.elapsed();

    let _conn2 = pool.get().await.unwrap();

    let events = events.lock();

    let id = match events[0] {
        Event::Acquire(ref event) => event.connection_id(),
        _ => unreachable!(),
    };

    match events[1] {
        Event::Checkout(ref event) => {
            assert_eq!(event.connection_id(), id);
            assert!(event.duration() <= checkout);
        }
        _ => unreachable!(),
    }

    match events[2] {
        Event::Timeout(ref event) => assert_eq!(event.timeout(), Duration::from_millis(123)),
        _ => unreachable!(),
    }

    match events[3] {
        Event::Checkin(ref event) => {
            assert_eq!(event.connection_id(), id);
            assert!(event.duration() <= checkin);
        }
        _ => unreachable!(),
    }

    match events[4] {
        Event::Release(ref event) => {
            assert_eq!(event.connection_id(), id);
            assert!(event.age() <= release);
        }
        _ => unreachable!(),
    }

    let id2 = match events[5] {
        Event::Acquire(ref event) => event.connection_id(),
        _ => unreachable!(),
    };
    assert!(id < id2);

    match events[6] {
        Event::Checkout(ref event) => assert_eq!(event.connection_id(), id2),
        _ => unreachable!(),
    }
}

#[runtime::test]
async fn extensions() {
    let pool = AsyncPool::builder()
        .max_size(2)
        .build(OkManager)
        .await
        .unwrap();

    let mut conn1 = pool.get().await.unwrap();
    let mut conn2 = pool.get().await.unwrap();

    AsyncPooledConnection::extensions_mut(&mut conn1).insert(1);
    AsyncPooledConnection::extensions_mut(&mut conn2).insert(2);

    drop(conn1);

    let conn = pool.get().await.unwrap();
    assert_eq!(
        AsyncPooledConnection::extensions(&conn).get::<i32>(),
        Some(&1)
    );
}
