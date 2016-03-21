use std::collections::BinaryHeap;
use std::cmp::{PartialOrd, Ord, PartialEq, Eq, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::time::{Duration, Instant};

use thunk::Thunk;

enum JobType {
    Once(Thunk<'static>),
    FixedRate {
        f: Box<FnMut() + Send + 'static>,
        rate: Duration,
    },
}

struct Job {
    type_: JobType,
    time: Instant,
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

impl SharedPool {
    fn run(&self, job: Job) {
        let mut inner = self.inner.lock().unwrap();

        // Calls from the pool itself will never hit this, but calls from workers might
        if inner.shutdown {
            return;
        }

        match inner.queue.peek() {
            None => self.cvar.notify_all(),
            Some(e) if e.time > job.time => self.cvar.notify_all(),
            _ => {}
        };
        inner.queue.push(job);
    }
}

pub struct ScheduledThreadPool {
    shared: Arc<SharedPool>,
}

impl Drop for ScheduledThreadPool {
    fn drop(&mut self) {
        self.shared.inner.lock().unwrap().shutdown = true;
        self.shared.cvar.notify_all();
    }
}

impl ScheduledThreadPool {
    pub fn new(size: usize) -> ScheduledThreadPool {
        assert!(size > 0, "size must be positive");

        let inner = InnerPool {
            queue: BinaryHeap::new(),
            shutdown: false,
        };

        let shared = SharedPool {
            inner: Mutex::new(inner),
            cvar: Condvar::new(),
        };

        let pool = ScheduledThreadPool { shared: Arc::new(shared) };

        for i in 0..size {
            Worker::start(i, pool.shared.clone());
        }

        pool
    }

    #[allow(dead_code)]
    pub fn run<F>(&self, job: F)
        where F: FnOnce() + Send + 'static
    {
        self.run_after(Duration::from_secs(0), job)
    }

    pub fn run_after<F>(&self, dur: Duration, job: F)
        where F: FnOnce() + Send + 'static
    {
        let job = Job {
            type_: JobType::Once(Thunk::new(job)),
            time: Instant::now() + dur,
        };
        self.shared.run(job)
    }

    pub fn run_at_fixed_rate<F>(&self, rate: Duration, f: F)
        where F: FnMut() + Send + 'static
    {
        let job = Job {
            type_: JobType::FixedRate {
                f: Box::new(f),
                rate: rate,
            },
            time: Instant::now() + rate,
        };
        self.shared.run(job)
    }
}

struct Worker {
    i: usize,
    shared: Arc<SharedPool>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Start up a new worker if this one's going away due to a panic from a job
        if thread::panicking() {
            Worker::start(self.i, self.shared.clone());
        }
    }
}

impl Worker {
    fn start(i: usize, shared: Arc<SharedPool>) {
        let mut worker = Worker {
            i: i,
            shared: shared,
        };
        thread::Builder::new()
            .name(format!("ScheduledThreadPool worker {}", i))
            .spawn(move || worker.run())
            .unwrap();
    }

    fn run(&mut self) {
        loop {
            match self.get_job() {
                Some(job) => self.run_job(job),
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
            let now = Instant::now();

            let need = match inner.queue.peek() {
                None if inner.shutdown => return None,
                None => Need::Wait,
                Some(e) if e.time <= now => break,
                Some(e) => Need::WaitTimeout(e.time - now),
            };

            inner = match need {
                Need::Wait => self.shared.cvar.wait(inner).unwrap(),
                Need::WaitTimeout(t) => self.shared.cvar.wait_timeout(inner, t).unwrap().0,
            };
        }

        Some(inner.queue.pop().unwrap())
    }

    fn run_job(&self, job: Job) {
        match job.type_ {
            JobType::Once(f) => f.invoke(()),
            JobType::FixedRate { mut f, rate } => {
                f();
                let new_job = Job {
                    type_: JobType::FixedRate { f: f, rate: rate },
                    time: job.time + rate,
                };
                self.shared.run(new_job)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::ScheduledThreadPool;

    const TEST_TASKS: usize = 4;

    #[test]
    fn test_works() {
        let pool = ScheduledThreadPool::new(TEST_TASKS);

        let (tx, rx) = channel();
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            pool.run(move || {
                tx.send(1usize).unwrap();
            });
        }

        assert_eq!(rx.iter().take(TEST_TASKS).fold(0, |a, b| a + b), TEST_TASKS);
    }

    #[test]
    #[should_panic(expected = "size must be positive")]
    fn test_zero_tasks_panic() {
        ScheduledThreadPool::new(0);
    }

    #[test]
    fn test_recovery_from_subtask_panic() {
        let pool = ScheduledThreadPool::new(TEST_TASKS);

        // Panic all the existing threads.
        let waiter = Arc::new(Barrier::new(TEST_TASKS as usize));
        for _ in 0..TEST_TASKS {
            let waiter = waiter.clone();
            pool.run(move || -> () {
                waiter.wait();
                panic!();
            });
        }

        // Ensure new threads were spawned to compensate.
        let (tx, rx) = channel();
        let waiter = Arc::new(Barrier::new(TEST_TASKS as usize));
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            let waiter = waiter.clone();
            pool.run(move || {
                waiter.wait();
                tx.send(1usize).unwrap();
            });
        }

        assert_eq!(rx.iter().take(TEST_TASKS).fold(0, |a, b| a + b), TEST_TASKS);
    }

    #[test]
    fn test_run_after() {
        let pool = ScheduledThreadPool::new(TEST_TASKS);
        let (tx, rx) = channel();

        let tx1 = tx.clone();
        pool.run_after(Duration::from_secs(1), move || tx1.send(1usize).unwrap());
        pool.run_after(Duration::from_millis(500),
                       move || tx.send(2usize).unwrap());

        assert_eq!(2, rx.recv().unwrap());
        assert_eq!(1, rx.recv().unwrap());
    }

    #[test]
    fn test_jobs_complete_after_drop() {
        let pool = ScheduledThreadPool::new(TEST_TASKS);
        let (tx, rx) = channel();

        let tx1 = tx.clone();
        pool.run_after(Duration::from_secs(1), move || tx1.send(1usize).unwrap());
        pool.run_after(Duration::from_millis(500),
                       move || tx.send(2usize).unwrap());

        drop(pool);

        assert_eq!(2, rx.recv().unwrap());
        assert_eq!(1, rx.recv().unwrap());
    }

    #[test]
    fn test_fixed_delay_jobs_stop_after_drop() {
        let pool = Arc::new(ScheduledThreadPool::new(TEST_TASKS));
        let (tx, rx) = channel();
        let (tx2, rx2) = channel();

        let mut pool2 = Some(pool.clone());
        let mut i = 0i32;
        pool.run_at_fixed_rate(Duration::from_millis(500), move || {
            i += 1;
            tx.send(i).unwrap();
            rx2.recv().unwrap();
            if i == 2 {
                drop(pool2.take().unwrap());
            }
        });
        drop(pool);

        assert_eq!(Ok(1), rx.recv());
        tx2.send(()).unwrap();
        assert_eq!(Ok(2), rx.recv());
        tx2.send(()).unwrap();
        assert!(rx.recv().is_err());
    }
}
