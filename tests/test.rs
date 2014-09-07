extern crate r2d2;

use std::comm;
use std::sync::{Mutex, Arc};
use std::sync::atomic::{AtomicBool, SeqCst};
use std::default::Default;

mod config;

#[deriving(Show, PartialEq)]
struct FakeConnection;

#[deriving(Default)]
struct OkManager;

impl r2d2::PoolManager<FakeConnection, ()> for OkManager {
    fn connect(&self) -> Result<FakeConnection, ()> {
        Ok(FakeConnection)
    }

    fn is_valid(&self, _: &FakeConnection) -> bool {
        true
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

    fn is_valid(&self, _: &FakeConnection) -> bool {
        true
    }
}

#[test]
fn test_pool_size_ok() {
    let config = r2d2::Config {
        pool_size: 5,
        ..Default::default()
    };
    let manager = NthConnectFailManager { n: Mutex::new(5) };
    assert!(r2d2::Pool::new(config, manager, r2d2::NoopErrorHandler).is_ok());
}

#[test]
fn test_pool_size_err() {
    let config = r2d2::Config {
        pool_size: 5,
        ..Default::default()
    };
    let manager = NthConnectFailManager { n: Mutex::new(4) };
    assert_eq!(r2d2::Pool::new(config, manager, r2d2::NoopErrorHandler).err().unwrap(),
               r2d2::ConnectionError(()));
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

        fn is_valid(&self, _: &FakeConnection) -> bool {
            if self.first.compare_and_swap(true, false, SeqCst) {
                self.s.lock().send(());
                self.r.lock().recv();
            }
            true
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
    spawn(proc() {
        p2.get().unwrap();
    });

    r1.recv();
    // get call by other task has triggered the health check
    pool.get().unwrap();
    s2.send(());
}
