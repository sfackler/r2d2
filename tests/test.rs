#![feature(std_misc, core)]
extern crate r2d2;

use std::default::Default;
use std::sync::atomic::{AtomicBool, ATOMIC_BOOL_INIT, Ordering};
use std::sync::mpsc::{self, SyncSender, Receiver};
use std::sync::{Mutex, Arc};
use std::thread::Thread;
use std::time::Duration;

mod config;

#[derive(Debug, PartialEq)]
struct FakeConnection;

struct OkManager;

impl r2d2::ConnectionManager for OkManager {
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

impl r2d2::ConnectionManager for NthConnectFailManager {
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
    let config = r2d2::config::Builder::new().pool_size(5).build();
    let manager = NthConnectFailManager { n: Mutex::new(5) };
    let pool = r2d2::Pool::new(config, manager, Box::new(r2d2::NoopErrorHandler)).unwrap();
    let mut conns = vec![];
    for _ in range(0, config.pool_size()) {
        conns.push(pool.get().ok().unwrap());
    }
}

#[test]
fn test_acquire_release() {
    let config = r2d2::config::Builder::new().pool_size(2).build();
    let pool = r2d2::Pool::new(config, OkManager, Box::new(r2d2::NoopErrorHandler)).unwrap();

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

    impl r2d2::ConnectionManager for BlockingChecker {
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

    let config = r2d2::config::Builder::new()
        .test_on_check_out(true)
        .pool_size(2)
        .build();
    let manager = BlockingChecker {
        first: AtomicBool::new(true),
        s: Mutex::new(s1),
        r: Mutex::new(r2),
    };
    let pool = Arc::new(r2d2::Pool::new(config, manager, Box::new(r2d2::NoopErrorHandler)).unwrap());

    let p2 = pool.clone();
    let _t = Thread::scoped(move || {
        p2.get().ok().unwrap();
    });

    r1.recv().unwrap();
    // get call by other task has triggered the health check
    pool.get().ok().unwrap();
    s2.send(()).ok().unwrap();
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

    impl r2d2::ConnectionManager for Handler {
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

    let pool = r2d2::Pool::new(Default::default(), Handler, Box::new(r2d2::NoopErrorHandler)).unwrap();

    drop(pool.get().ok().unwrap());

    assert!(DROPPED.load(Ordering::SeqCst));
}

#[test]
fn test_initialization_failure() {
    let config = r2d2::config::Builder::new()
        .connection_timeout(Duration::seconds(1))
        .build();
    let manager = NthConnectFailManager {
        n: Mutex::new(0),
    };
    r2d2::Pool::new(config, manager, Box::new(r2d2::NoopErrorHandler)).err().unwrap();
}

#[test]
fn test_get_timeout() {
    let config = r2d2::config::Builder::new()
        .pool_size(1)
        .connection_timeout(Duration::seconds(1))
        .build();
    let pool = r2d2::Pool::new(config, OkManager, Box::new(r2d2::NoopErrorHandler)).unwrap();
    let _c = pool.get().unwrap();
    pool.get().err().unwrap();
}
