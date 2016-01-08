use std::sync::atomic::{AtomicBool, ATOMIC_BOOL_INIT, AtomicUsize, ATOMIC_USIZE_INIT, AtomicIsize,
                        Ordering};
use std::sync::mpsc::{self, SyncSender, Receiver};
use std::sync::{Mutex, Arc};
use std::time::Duration;
use std::thread;

use {ManageConnection, CustomizeConnection, Pool, Config};

#[derive(Debug, PartialEq)]
struct FakeConnection(bool);

struct OkManager;

impl ManageConnection for OkManager {
    type Connection = FakeConnection;
    type Error = ();

    fn connect(&self) -> Result<FakeConnection, ()> {
        Ok(FakeConnection(true))
    }

    fn is_valid(&self, _: &mut FakeConnection) -> Result<(), ()> {
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
    type Error = ();

    fn connect(&self) -> Result<FakeConnection, ()> {
        let mut n = self.n.lock().unwrap();
        if *n > 0 {
            *n -= 1;
            Ok(FakeConnection(true))
        } else {
            Err(())
        }
    }

    fn is_valid(&self, _: &mut FakeConnection) -> Result<(), ()> {
        Ok(())
    }

    fn has_broken(&self, _: &mut FakeConnection) -> bool {
        false
    }
}

#[test]
fn test_pool_size_ok() {
    let config = Config::builder().pool_size(5).build();
    let manager = NthConnectFailManager { n: Mutex::new(5) };
    let pool = Pool::new(config, manager).unwrap();
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().ok().unwrap());
    }
}

#[test]
fn test_acquire_release() {
    let config = Config::builder().pool_size(2).build();
    let pool = Pool::new(config, OkManager).unwrap();

    let conn1 = pool.get().ok().unwrap();
    let conn2 = pool.get().ok().unwrap();
    drop(conn1);
    let conn3 = pool.get().ok().unwrap();
    drop(conn2);
    drop(conn3);
}

#[test]
fn test_is_send_sync() {
    fn is_send_sync<T: Send+Sync>() {}
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
        type Error = ();

        fn connect(&self) -> Result<FakeConnection, ()> {
            Ok(FakeConnection(true))
        }

        fn is_valid(&self, _: &mut FakeConnection) -> Result<(), ()> {
            if self.first.compare_and_swap(true, false, Ordering::SeqCst) {
                self.s.lock().unwrap().send(()).unwrap();
                self.r.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }

        fn has_broken(&self, _: &mut FakeConnection) -> bool {
            false
        }
    }

    let (s1, r1) = mpsc::sync_channel(0);
    let (s2, r2) = mpsc::sync_channel(0);

    let config = Config::builder()
        .test_on_check_out(true)
        .pool_size(2)
        .build();
    let manager = BlockingChecker {
        first: AtomicBool::new(true),
        s: Mutex::new(s1),
        r: Mutex::new(r2),
    };
    let pool = Arc::new(Pool::new(config, manager).unwrap());

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
        type Error = ();

        fn connect(&self) -> Result<Connection, ()> {
            Ok(Connection)
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), ()> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            true
        }
    }

    let pool = Pool::new(Default::default(), Handler).unwrap();

    drop(pool.get().ok().unwrap());

    assert!(DROPPED.load(Ordering::SeqCst));
}

#[test]
fn test_initialization_failure() {
    let config = Config::builder()
        .connection_timeout_ms(1000)
        .build();
    let manager = NthConnectFailManager {
        n: Mutex::new(0),
    };
    Pool::new(config, manager).err().unwrap();
}

#[test]
fn test_get_timeout() {
    let config = Config::builder()
        .pool_size(1)
        .connection_timeout(Duration::from_secs(1))
        .build();
    let pool = Pool::new(config, OkManager).unwrap();
    let _c = pool.get().unwrap();
    pool.get().err().unwrap();
}

#[test]
fn test_connection_customizer() {
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
        type Error = ();

        fn connect(&self) -> Result<Connection, ()> {
            Ok(Connection(0))
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), ()> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            true
        }
    }

    struct Customizer;

    impl CustomizeConnection<Connection, ()> for Customizer {
        fn on_acquire(&self, conn: &mut Connection) -> Result<(), ()> {
            if !DROPPED.load(Ordering::SeqCst) {
                Err(())
            } else {
                conn.0 = 1;
                Ok(())
            }
        }
    }

    let config = Config::builder()
        .connection_customizer(Box::new(Customizer))
        .build();
    let pool = Pool::new(config, Handler).unwrap();

    let conn = pool.get().unwrap();
    assert_eq!(1, conn.0);
    assert!(DROPPED.load(Ordering::SeqCst));
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
        type Error = ();

        fn connect(&self) -> Result<Connection, ()> {
            if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                Ok(Connection)
            } else {
                Err(())
            }
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), ()> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let config = Config::builder()
        .pool_size(5)
        .idle_timeout(Some(Duration::from_secs(1)))
        .build();
    let pool = Pool::new_inner(config, Handler(AtomicIsize::new(5)), 1).unwrap();
    let conn = pool.get().unwrap();
    thread::sleep(Duration::from_secs(2));
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    drop(conn);
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
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
        type Error = ();

        fn connect(&self) -> Result<Connection, ()> {
            if self.0.fetch_sub(1, Ordering::SeqCst) > 0 {
                Ok(Connection)
            } else {
                Err(())
            }
        }

        fn is_valid(&self, _: &mut Connection) -> Result<(), ()> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Connection) -> bool {
            false
        }
    }

    let config = Config::builder()
        .pool_size(5)
        .max_lifetime(Some(Duration::from_secs(1)))
        .connection_timeout(Duration::from_secs(1))
        .build();
    let pool = Pool::new_inner(config, Handler(AtomicIsize::new(5)), 1).unwrap();
    let conn = pool.get().unwrap();
    thread::sleep(Duration::from_secs(2));
    assert_eq!(4, DROPPED.load(Ordering::SeqCst));
    drop(conn);
    thread::sleep(Duration::from_secs(2));
    assert_eq!(5, DROPPED.load(Ordering::SeqCst));
    assert!(pool.get().is_err());
}
