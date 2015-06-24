extern crate r2d2;

use std::default::Default;
use std::sync::atomic::{AtomicBool, ATOMIC_BOOL_INIT, Ordering};
use std::sync::mpsc::{self, SyncSender, Receiver};
use std::sync::{Mutex, Arc};
use std::thread;

mod config;

#[derive(Debug, PartialEq)]
struct FakeConnection;

struct OkManager;

impl r2d2::ManageConnection for OkManager {
    type Connection = FakeConnection;
    type Error = ();

    fn connect(&self) -> Result<FakeConnection, ()> {
        Ok(FakeConnection)
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

impl r2d2::ManageConnection for NthConnectFailManager {
    type Connection = FakeConnection;
    type Error = ();

    fn connect(&self) -> Result<FakeConnection, ()> {
        let mut n = self.n.lock().unwrap();
        if *n > 0 {
            *n -= 1;
            Ok(FakeConnection)
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
    let config = r2d2::Config::builder().pool_size(5).build();
    let manager = NthConnectFailManager { n: Mutex::new(5) };
    let pool = r2d2::Pool::new(config, manager).unwrap();
    let mut conns = vec![];
    for _ in 0..5 {
        conns.push(pool.get().ok().unwrap());
    }
}

#[test]
fn test_acquire_release() {
    let config = r2d2::Config::builder().pool_size(2).build();
    let pool = r2d2::Pool::new(config, OkManager).unwrap();

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
    is_send_sync::<r2d2::Pool<OkManager>>();
}

#[test]
fn test_issue_2_unlocked_during_is_valid() {
    struct BlockingChecker {
        first: AtomicBool,
        s: Mutex<SyncSender<()>>,
        r: Mutex<Receiver<()>>,
    }

    impl r2d2::ManageConnection for BlockingChecker {
        type Connection = FakeConnection;
        type Error = ();

        fn connect(&self) -> Result<FakeConnection, ()> {
            Ok(FakeConnection)
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

    let config = r2d2::Config::builder()
        .test_on_check_out(true)
        .pool_size(2)
        .build();
    let manager = BlockingChecker {
        first: AtomicBool::new(true),
        s: Mutex::new(s1),
        r: Mutex::new(r2),
    };
    let pool = Arc::new(r2d2::Pool::new(config, manager).unwrap());

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

    impl r2d2::ManageConnection for Handler {
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

    let pool = r2d2::Pool::new(Default::default(), Handler).unwrap();

    drop(pool.get().ok().unwrap());

    assert!(DROPPED.load(Ordering::SeqCst));
}

#[test]
fn test_initialization_failure() {
    let config = r2d2::Config::builder()
        .connection_timeout_ms(1000)
        .build();
    let manager = NthConnectFailManager {
        n: Mutex::new(0),
    };
    r2d2::Pool::new(config, manager).err().unwrap();
}

#[test]
fn test_get_timeout() {
    let config = r2d2::Config::builder()
        .pool_size(1)
        .connection_timeout_ms(1000)
        .build();
    let pool = r2d2::Pool::new(config, OkManager).unwrap();
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

    impl r2d2::ManageConnection for Handler {
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

    impl r2d2::CustomizeConnection<Connection, ()> for Customizer {
        fn on_acquire(&self, conn: &mut Connection) -> Result<(), ()> {
            if !DROPPED.load(Ordering::SeqCst) {
                Err(())
            } else {
                conn.0 = 1;
                Ok(())
            }
        }
    }

    let config = r2d2::Config::builder()
        .connection_customizer(Box::new(Customizer))
        .build();
    let pool = r2d2::Pool::new(config, Handler).unwrap();

    let conn = pool.get().unwrap();
    assert_eq!(1, conn.0);
    assert!(DROPPED.load(Ordering::SeqCst));
}
