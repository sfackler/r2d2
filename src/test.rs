use antidote::Mutex;
use std::sync::atomic::{
    AtomicBool, AtomicIsize, AtomicUsize, Ordering, ATOMIC_BOOL_INIT, ATOMIC_USIZE_INIT,
};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::Duration;
use std::{error, fmt, mem, thread};

use {CustomizeConnection, ManageConnection, Pool};

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

impl ManageConnection for OkManager {
    type Connection = FakeConnection;
    type Error = Error;

    fn connect(&self) -> Result<FakeConnection, Error> {
        Ok(FakeConnection(true))
    }

    fn is_valid(&self, _: &mut FakeConnection) -> Result<(), Error> {
        Ok(())
    }

    fn has_broken(&self, _: &mut FakeConnection) -> bool {
        false
    }
}

struct NthConnectFailManager {
    n: Mutex<u32>,
}

impl ManageConnection for NthConnectFailManager {
    type Connection = FakeConnection;
    type Error = Error;

    fn connect(&self) -> Result<FakeConnection, Error> {
        let mut n = self.n.lock();
        if *n > 0 {
            *n -= 1;
            Ok(FakeConnection(true))
        } else {
            Err(Error)
        }
    }

    fn is_valid(&self, _: &mut FakeConnection) -> Result<(), Error> {
        Ok(())
    }

    fn has_broken(&self, _: &mut FakeConnection) -> bool {
        false
    }
}

#[test]
fn test_max_size_ok() {
    let manager = NthConnectFailManager { n: Mutex::new(5) };
    let pool = Pool::builder().max_size(5).build(manager).unwrap();
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().ok().unwrap());
    }
}

#[test]
fn test_acquire_release() {
    let pool = Pool::builder().max_size(2).build(OkManager).unwrap();

    let conn1 = pool.get().ok().unwrap();
    let conn2 = pool.get().ok().unwrap();
    drop(conn1);
    let conn3 = pool.get().ok().unwrap();
    drop(conn2);
    drop(conn3);
}

#[test]
fn try_get() {
    let pool = Pool::builder().max_size(2).build(OkManager).unwrap();

    let conn1 = pool.try_get();
    let conn2 = pool.try_get();
    let conn3 = pool.try_get();

    assert!(conn1.is_some());
    assert!(conn2.is_some());
    assert!(conn3.is_none());

    drop(conn1);

    assert!(pool.try_get().is_some());
}

#[test]
fn test_is_send_sync() {
    fn is_send_sync<T: Send + Sync>() {}
    is_send_sync::<Pool<OkManager>>();
}

#[test]
fn test_issue_2_unlocked_during_is_valid() {
    struct BlockingChecker {
        first: AtomicBool,
        s: Mutex<SyncSender<()>>,
        r: Mutex<Receiver<()>>,
    }

    impl ManageConnection for BlockingChecker {
        type Connection = FakeConnection;
        type Error = Error;

        fn connect(&self) -> Result<FakeConnection, Error> {
            Ok(FakeConnection(true))
        }

        fn is_valid(&self, _: &mut FakeConnection) -> Result<(), Error> {
            if self.first.compare_and_swap(true, false, Ordering::SeqCst) {
                self.s.lock().send(()).unwrap();
                self.r.lock().recv().unwrap();
            }
            Ok(())
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

    let pool = Pool::builder()
        .test_on_check_out(true)
        .max_size(2)
        .build(manager)
        .unwrap();

    let p2 = pool.clone();
    let t = thread::spawn(move || {
        p2.get().ok().unwrap();
    });

    r1.recv().unwrap();
    // get call by other task has triggered the health check
    pool.get().ok().unwrap();
    s2.send(()).ok().unwrap();

    t.join().ok().unwrap();
}

#[test]
fn test_drop_on_broken() {
    static DROPPED: AtomicBool = ATOMIC_BOOL_INIT;
    DROPPED.store(false, Ordering::SeqCst);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }

    struct Handler;

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Result<Connection, Error> {
            Ok(Connection)
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            true
        }
    }

    let pool = Pool::new(Handler).unwrap();

    drop(pool.get().ok().unwrap());

    assert!(DROPPED.load(Ordering::SeqCst));
}

#[test]
fn test_initialization_failure() {
    let manager = NthConnectFailManager { n: Mutex::new(0) };
    let err = Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .build(manager)
        .err()
        .unwrap();
    assert!(err.to_string().contains("blammo"));
}

#[test]
fn test_lazy_initialization_failure() {
    let manager = NthConnectFailManager { n: Mutex::new(0) };
    let pool = Pool::builder()
        .connection_timeout(Duration::from_secs(1))
        .build_unchecked(manager);
    let err = pool.get().err().unwrap();
    assert!(err.to_string().contains("blammo"));
}

#[test]
fn test_get_timeout() {
    let pool = Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_secs(1))
        .build(OkManager)
        .unwrap();
    let _c = pool.get().unwrap();
    pool.get().err().unwrap();
}

#[test]
fn test_connection_customizer() {
    static RELEASED: AtomicBool = ATOMIC_BOOL_INIT;
    RELEASED.store(false, Ordering::SeqCst);

    static DROPPED: AtomicBool = ATOMIC_BOOL_INIT;
    DROPPED.store(false, Ordering::SeqCst);

    struct Connection(i32);

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, Ordering::SeqCst);
        }
    }

    struct Handler;

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Result<Connection, Error> {
            Ok(Connection(0))
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            true
        }
    }

    #[derive(Debug)]
    struct Customizer;

    impl CustomizeConnection<Connection, Error> for Customizer {
        fn on_acquire(&self, conn: &mut Connection) -> Result<(), Error> {
            if !DROPPED.load(Ordering::SeqCst) {
                Err(Error)
            } else {
                conn.0 = 1;
                Ok(())
            }
        }

        fn on_release(&self, _: Connection) {
            RELEASED.store(true, Ordering::SeqCst);
        }
    }

    let pool = Pool::builder()
        .connection_customizer(Box::new(Customizer))
        .build(Handler)
        .unwrap();

    {
        let conn = pool.get().unwrap();
        assert_eq!(1, conn.0);
        assert!(!RELEASED.load(Ordering::SeqCst));
        assert!(DROPPED.load(Ordering::SeqCst));
    }
    assert!(RELEASED.load(Ordering::SeqCst));
}

#[test]
fn test_idle_timeout() {
    static DROPPED: AtomicUsize = ATOMIC_USIZE_INIT;

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler(AtomicIsize);

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Result<Connection, Error> {
            if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                Ok(Connection)
            } else {
                Err(Error)
            }
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder()
        .max_size(5)
        .idle_timeout(Some(Duration::from_secs(1)))
        .reaper_rate(Duration::from_secs(1))
        .build(Handler(AtomicIsize::new(5)))
        .unwrap();
    let conn = pool.get().unwrap();
    thread::sleep(Duration::from_secs(2));
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    drop(conn);
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
}

#[test]
fn idle_timeout_partial_use() {
    static DROPPED: AtomicUsize = ATOMIC_USIZE_INIT;

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler(AtomicIsize);

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Result<Connection, Error> {
            if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                Ok(Connection)
            } else {
                Err(Error)
            }
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder()
        .max_size(5)
        .idle_timeout(Some(Duration::from_secs(1)))
        .reaper_rate(Duration::from_secs(1))
        .build(Handler(AtomicIsize::new(5)))
        .unwrap();
    for _ in 0..8 {
        thread::sleep(Duration::from_millis(250));
        pool.get().unwrap();
    }
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    assert_eq!(1, pool.state().connections);
}

#[test]
fn test_max_lifetime() {
    static DROPPED: AtomicUsize = ATOMIC_USIZE_INIT;

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler(AtomicIsize);

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Result<Connection, Error> {
            if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                Ok(Connection)
            } else {
                Err(Error)
            }
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder()
        .max_size(5)
        .max_lifetime(Some(Duration::from_secs(1)))
        .connection_timeout(Duration::from_secs(1))
        .reaper_rate(Duration::from_secs(1))
        .build(Handler(AtomicIsize::new(5)))
        .unwrap();
    let conn = pool.get().unwrap();
    thread::sleep(Duration::from_secs(2));
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    drop(conn);
    thread::sleep(Duration::from_secs(2));
    assert_eq!(5, DROPPED.load(Ordering::SeqCst));
    assert!(pool.get().is_err());
}

#[test]
fn min_idle() {
    struct Connection;

    struct Handler;

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Result<Connection, Error> {
            Ok(Connection)
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder()
        .max_size(5)
        .min_idle(Some(2))
        .build(Handler)
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    assert_eq!(2, pool.state().idle_connections);
    assert_eq!(2, pool.state().connections);
    let conns = (0..3).map(|_| pool.get().unwrap()).collect::<Vec<_>>();
    thread::sleep(Duration::from_secs(1));
    assert_eq!(2, pool.state().idle_connections);
    assert_eq!(5, pool.state().connections);
    mem::drop(conns);
    assert_eq!(5, pool.state().idle_connections);
    assert_eq!(5, pool.state().connections);
}

#[test]
fn conns_drop_on_pool_drop() {
    static DROPPED: AtomicUsize = ATOMIC_USIZE_INIT;

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Handler;

    impl ManageConnection for Handler {
        type Connection = Connection;
        type Error = Error;

        fn connect(&self) -> Result<Connection, Error> {
            Ok(Connection)
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), Error> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let pool = Pool::builder()
        .max_lifetime(Some(Duration::from_secs(10)))
        .max_size(10)
        .build(Handler)
        .unwrap();
    drop(pool);
    for _ in 0..10 {
        if DROPPED.load(Ordering::SeqCst) == 10 {
            return;
        }
        thread::sleep(Duration::from_secs(1));
    }
    panic!("timed out waiting for connections to drop");
}
