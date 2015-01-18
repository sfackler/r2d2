use std::collections::BinaryHeap;
use std::cmp::{PartialOrd, Ord, PartialEq, Eq, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use std::thread::Thread;
use std::thunk::Thunk;
use std::time::Duration;

use time;

struct Job {
    thunk: Thunk,
    time: u64,
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Job) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Job) -> Ordering {
        // reverse because BinaryHeap's a max heap
        self.time.cmp(&other.time).reverse()
    }
}

impl PartialEq for Job {
    fn eq(&self, other: &Job) -> bool {
        self.time == other.time
    }
}

impl Eq for Job {}

struct InnerPool {
    queue: BinaryHeap<Job>,
    shutdown: bool,
}

struct SharedPool {
    inner: Mutex<InnerPool>,
    cvar: Condvar,
}

pub struct ScheduledTaskPool {
    shared: Arc<SharedPool>,
}

impl Drop for ScheduledTaskPool {
    fn drop(&mut self) {
        self.shared.inner.lock().unwrap().shutdown = true;
        self.shared.cvar.notify_all();
    }
}

impl ScheduledTaskPool {
    pub fn new(size: usize) -> ScheduledTaskPool {
        assert!(size > 0, "size must be positive");

        let inner = InnerPool {
            queue: BinaryHeap::new(),
            shutdown: false,
        };

        let shared = SharedPool {
            inner: Mutex::new(inner),
            cvar: Condvar::new(),
        };

        let pool = ScheduledTaskPool {
            shared: Arc::new(shared),
        };

        for _ in (0..size) {
            let mut worker = Worker {
                shared: pool.shared.clone(),
            };

            Thread::spawn(move || worker.run());
        }

        pool
    }

    pub fn execute<F>(&self, f: F) where F: FnOnce() + Send {
        self.execute_after(Duration::zero(), f)
    }

    pub fn execute_after<F>(&self, dur: Duration, f: F) where F: FnOnce() + Send {
        let job = Job {
            thunk: Thunk::new(f),
            time: (time::precise_time_ns() as i64 + dur.num_nanoseconds().unwrap()) as u64,
        };
        let mut inner = self.shared.inner.lock().unwrap();
        match inner.queue.peek() {
            None => self.shared.cvar.notify_all(),
            Some(e) if e.time > job.time => self.shared.cvar.notify_all(),
            _ => {}
        };
        inner.queue.push(job);
    }
}

struct Worker {
    shared: Arc<SharedPool>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Start up a new worker if this one's going away due to a panic from a job
        if Thread::panicking() {
            let mut worker = Worker {
                shared: self.shared.clone(),
            };
            Thread::spawn(move || worker.run());
        }
    }
}

impl Worker {
    fn run(&mut self) {
        loop {
            match self.get_job() {
                Some(job) => job.thunk.invoke(()),
                None => break,
            }
        }
    }

    fn get_job(&self) -> Option<Job> {
        enum Need {
            Wait,
            WaitTimeout(Duration),
        }

        let mut inner = self.shared.inner.lock().unwrap();
        loop {
            let now = time::precise_time_ns();

            let need = match inner.queue.peek() {
                None if inner.shutdown => return None,
                None => Need::Wait,
                Some(e) if e.time <= now => break,
                Some(e) => Need::WaitTimeout(Duration::nanoseconds(e.time as i64 - now as i64)),
            };

            inner = match need {
                Need::Wait => self.shared.cvar.wait(inner).unwrap(),
                Need::WaitTimeout(t) => self.shared.cvar.wait_timeout(inner, t).unwrap().0,
            };
        }

        Some(inner.queue.pop().unwrap())
    }
}

#[cfg(test)]
mod test {
    use std::iter::AdditiveIterator;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::ScheduledTaskPool;

    const TEST_TASKS: usize = 4;

    #[test]
    fn test_works() {
        let pool = ScheduledTaskPool::new(TEST_TASKS);

        let (tx, rx) = channel();
        for _ in range(0, TEST_TASKS) {
            let tx = tx.clone();
            pool.execute(move|| {
                tx.send(1us).unwrap();
            });
        }

        assert_eq!(rx.iter().take(TEST_TASKS).sum(), TEST_TASKS);
    }

    #[test]
    #[should_fail(expected = "size must be positive")]
    fn test_zero_tasks_panic() {
        ScheduledTaskPool::new(0);
    }

    #[test]
    fn test_recovery_from_subtask_panic() {
        let pool = ScheduledTaskPool::new(TEST_TASKS);

        // Panic all the existing threads.
        let waiter = Arc::new(Barrier::new(TEST_TASKS as usize));
        for _ in range(0, TEST_TASKS) {
            let waiter = waiter.clone();
            pool.execute(move || -> () {
                waiter.wait();
                panic!();
            });
        }

        // Ensure new threads were spawned to compensate.
        let (tx, rx) = channel();
        let waiter = Arc::new(Barrier::new(TEST_TASKS as usize));
        for _ in range(0, TEST_TASKS) {
            let tx = tx.clone();
            let waiter = waiter.clone();
            pool.execute(move || {
                waiter.wait();
                tx.send(1us).unwrap();
            });
        }

        assert_eq!(rx.iter().take(TEST_TASKS).sum(), TEST_TASKS);
    }

    #[test]
    fn test_execute_after() {
        let pool = ScheduledTaskPool::new(TEST_TASKS);
        let (tx, rx) = channel();

        let tx1 = tx.clone();
        pool.execute_after(Duration::seconds(1), move || tx1.send(1us).unwrap());
        pool.execute_after(Duration::milliseconds(500), move || tx.send(2us).unwrap());

        assert_eq!(2, rx.recv().unwrap());
        assert_eq!(1, rx.recv().unwrap());
    }

    #[test]
    fn test_jobs_complete_after_drop() {
        let pool = ScheduledTaskPool::new(TEST_TASKS);
        let (tx, rx) = channel();

        let tx1 = tx.clone();
        pool.execute_after(Duration::seconds(1), move || tx1.send(1us).unwrap());
        pool.execute_after(Duration::milliseconds(500), move || tx.send(2us).unwrap());

        drop(pool);

        assert_eq!(2, rx.recv().unwrap());
        assert_eq!(1, rx.recv().unwrap());
    }
}
