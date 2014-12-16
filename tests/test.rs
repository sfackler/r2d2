extern crate r2d2;

use std::comm;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicBool, INIT_ATOMIC_BOOL, SeqCst};
use std::default::Default;

use r2d2::ErrorHandler;

mod config;

#[deriving(Show, PartialEq)]
struct FakeConnection;

struct OkManager;

impl r2d2::PoolManager<FakeConnection, ()> for OkManager {
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
    n: Mutex<uint>,
}

impl r2d2::PoolManager<FakeConnection, ()> for NthConnectFailManager {
    fn connect(&self) -> Result<FakeConnection, ()> {
        let mut n = self.n.lock();
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
    let config = r2d2::Config {
        pool_size: 5,
        ..Default::default()
    };
    let manager = NthConnectFailManager { n: Mutex::new(5) };
    let pool = r2d2::Pool::new(config, manager, r2d2::NoopErrorHandler).unwrap();
    let mut conns = vec![];
    for _ in range(0, config.pool_size) {
        conns.push(pool.get().unwrap());
    }
}

#[test]
fn test_acquire_release() {
    let config = r2d2::Config {
        pool_size: 2,
        ..Default::default()
    };
    let pool = r2d2::Pool::new(config, OkManager, r2d2::NoopErrorHandler).unwrap();

    let conn1 = pool.get().unwrap();
    let conn2 = pool.get().unwrap();
    drop(conn1);
    let conn3 = pool.get().unwrap();
    drop(conn2);
    drop(conn3);
}

#[test]
fn test_is_send_sync() {
    fn is_send_sync<T: Send+Sync>() {}
    is_send_sync::<r2d2::Pool<FakeConnection, (), OkManager, r2d2::NoopErrorHandler>>();
}

#[test]
fn test_issue_2_unlocked_during_is_valid() {
    struct BlockingChecker {
        first: AtomicBool,
        s: Mutex<SyncSender<()>>,
        r: Mutex<Receiver<()>>,
    }

    impl r2d2::PoolManager<FakeConnection, ()> for BlockingChecker {
        fn connect(&self) -> Result<FakeConnection, ()> {
            Ok(FakeConnection)
        }

        fn is_valid(&self, _: &mut FakeConnection) -> Result<(), ()> {
            if self.first.compare_and_swap(true, false, SeqCst) {
                self.s.lock().send(());
                self.r.lock().recv();
            }
            Ok(())
        }

        fn has_broken(&self, _: &mut FakeConnection) -> bool {
            false
        }
    }

    let (s1, r1) = comm::sync_channel(0);
    let (s2, r2) = comm::sync_channel(0);

    let config = r2d2::Config {
        test_on_check_out: true,
        pool_size: 2,
        ..Default::default()
    };
    let manager = BlockingChecker {
        first: AtomicBool::new(true),
        s: Mutex::new(s1),
        r: Mutex::new(r2),
    };
    let pool = Arc::new(r2d2::Pool::new(config, manager, r2d2::NoopErrorHandler).unwrap());

    let p2 = pool.clone();
    spawn(move || {
        p2.get().unwrap();
    });

    r1.recv();
    // get call by other task has triggered the health check
    pool.get().unwrap();
    s2.send(());
}

#[test]
fn test_drop_on_broken() {
    static DROPPED: AtomicBool = INIT_ATOMIC_BOOL;
    DROPPED.store(false, SeqCst);

    struct Connection;

    impl Drop for Connection {
        fn drop(&mut self) {
            DROPPED.store(true, SeqCst);
        }
    }

    struct Handler;

    impl r2d2::PoolManager<Connection, ()> for Handler {
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

    let pool = r2d2::Pool::new(Default::default(), Handler, r2d2::NoopErrorHandler).unwrap();

    drop(pool.get().unwrap());

    assert!(DROPPED.load(SeqCst));
}

// Just make sure that a boxed error handler works and doesn't self recurse or anything
#[test]
fn test_boxed_error_handler() {
    let handler: Box<ErrorHandler<()>> = box r2d2::NoopErrorHandler;
    handler.handle_error(());
    r2d2::Pool::new(Default::default(), OkManager, handler).unwrap();
}
