//! Pool configuration.
use std::fmt;
use std::time::Duration;

use {HandleError, NopErrorHandler, CustomizeConnection, NopConnectionCustomizer};

/// A builder for `Config`.
///
/// See the documentation of `Config` for more details about the default value
/// and meaning of the configuration parameters.
#[derive(Debug)]
pub struct Builder<C, E> {
    c: Config<C, E>,
}

impl<C, E> Builder<C, E> {
    /// Constructs a new `Builder`.
    ///
    /// Parameters are initialized with their default values.
    pub fn new() -> Builder<C, E> {
        Builder {
            c: Config::default(),
        }
    }

    /// Sets `pool_size`.
    ///
    /// # Panics
    ///
    /// Panics if `pool_size` is 0.
    pub fn pool_size(mut self, pool_size: u32) -> Builder<C, E> {
        assert!(pool_size > 0, "pool_size must be positive");
        self.c.pool_size = pool_size;
        self
    }

    /// Sets `helper_threads`.
    ///
    /// # Panics
    ///
    /// Panics if `helper_threads` is 0.
    pub fn helper_threads(mut self, helper_threads: u32) -> Builder<C, E> {
        assert!(helper_threads > 0, "helper_threads must be positive");
        self.c.helper_threads = helper_threads;
        self
    }

    /// Sets `test_on_check_out`.
    pub fn test_on_check_out(mut self, test_on_check_out: bool) -> Builder<C, E> {
        self.c.test_on_check_out = test_on_check_out;
        self
    }

    /// Sets `initialization_fail_fast`.
    pub fn initialization_fail_fast(mut self, initialization_fail_fast: bool) -> Builder<C, E> {
        self.c.initialization_fail_fast = initialization_fail_fast;
        self
    }

    /// Sets `idle_timeout`.
    ///
    /// # Panics
    ///
    /// Panics if `idle_timeout` is set to the zero `Duration`.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Builder<C, E> {
        assert!(idle_timeout != Some(Duration::from_secs(0)), "idle_timeout must be nonzero");
        self.c.idle_timeout = idle_timeout;
        self
    }

    /// Sets `connection_timeout` to the specified duration.
    ///
    /// # Panics
    ///
    /// Panics if `connection_timeout` is the zero duration
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Builder<C, E> {
        assert!(connection_timeout.as_secs() > 0 || connection_timeout.subsec_nanos() > 0,
                "connection_timeout must be positive");
        self.c.connection_timeout = connection_timeout;
        self
    }

    /// # Deprecated
    ///
    /// Use `connection_timeout` instead.
    pub fn connection_timeout_ms(self, connection_timeout_ms: u32) -> Builder<C, E> {
        self.connection_timeout(Duration::from_millis(connection_timeout_ms as u64))
    }

    /// Sets the `error_handler`.
    pub fn error_handler(mut self, error_handler: Box<HandleError<E>>) -> Builder<C, E> {
        self.c.error_handler = error_handler;
        self
    }

    /// Sets the `connection_customizer`.
    pub fn connection_customizer(mut self, connection_customizer: Box<CustomizeConnection<C, E>>)
                                 -> Builder<C, E> {
        self.c.connection_customizer = connection_customizer;
        self
    }

    /// Consumes the `Builder`, turning it into a `Config`.
    pub fn build(self) -> Config<C, E> {
        self.c
    }
}

/// A struct specifying the runtime configuration of a pool.
///
/// `Config` implements `Default`, which provides a set of reasonable default
/// values. It can be constructed using a `Builder`.
pub struct Config<C, E> {
    pool_size: u32,
    helper_threads: u32,
    test_on_check_out: bool,
    initialization_fail_fast: bool,
    idle_timeout: Option<Duration>,
    connection_timeout: Duration,
    error_handler: Box<HandleError<E>>,
    connection_customizer: Box<CustomizeConnection<C, E>>,
}

impl<C, E> fmt::Debug for Config<C, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Config")
            .field("pool_size", &self.pool_size)
            .field("helper_threads", &self.helper_threads)
            .field("test_on_check_out", &self.test_on_check_out)
            .field("initialization_fail_fast", &self.initialization_fail_fast)
            .field("idle_timeout", &self.idle_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .finish()
    }
}

impl<C, E> Default for Config<C, E> {
    fn default() -> Config<C, E> {
        Config {
            pool_size: 10,
            helper_threads: 3,
            test_on_check_out: true,
            initialization_fail_fast: true,
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            connection_timeout: Duration::from_secs(30),
            error_handler: Box::new(NopErrorHandler),
            connection_customizer: Box::new(NopConnectionCustomizer),
        }
    }
}

impl<C, E> Config<C, E> {
    /// Creates a new `Builder` which can be used to construct a customized
    /// `Config`.
    ///
    /// All parameters are initialized to their default values.
    pub fn builder() -> Builder<C, E> {
        Builder::new()
    }

    /// The number of connections managed by the pool.
    ///
    /// Defaults to 10.
    pub fn pool_size(&self) -> u32 {
        self.pool_size
    }

    /// The number of threads that the pool will use for asynchronous
    /// operations such as connection creation and health checks.
    ///
    /// Defaults to 3.
    pub fn helper_threads(&self) -> u32 {
        self.helper_threads
    }

    /// If true, the health of a connection will be verified via a call to
    /// `ConnectionManager::is_valid` before it is checked out of the pool.
    ///
    /// Defaults to true.
    pub fn test_on_check_out(&self) -> bool {
        self.test_on_check_out
    }

    /// If true, `Pool::new` will synchronously initialize its connections,
    /// returning an error if they could not be created.
    ///
    /// Defaults to true.
    pub fn initialization_fail_fast(&self) -> bool {
        self.initialization_fail_fast
    }

    /// If set, connections will be closed after sitting idle for this long.
    ///
    /// Defaults to 10 minutes.
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    /// Calls to `Pool::get` will wait this long for a connection to become
    /// available before returning an error.
    ///
    /// Defaults to 30 seconds.
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    /// # Deprecated
    ///
    /// Use `connection_timeout` instead.
    pub fn connection_timeout_ms(&self) -> u32 {
        self.connection_timeout.as_secs() as u32 * 1000 +
            self.connection_timeout.subsec_nanos() / 1_000_000
    }

    /// The handler for error reported in the pool.
    ///
    /// Defaults to `r2d2::NopErrorHandler`.
    pub fn error_handler(&self) -> &HandleError<E> {
        &*self.error_handler
    }

    /// The connection customizer used by the pool.
    ///
    /// Defaults to `r2d2::NopConnectionCustomizer`.
    pub fn connection_customizer(&self) -> &CustomizeConnection<C, E> {
        &*self.connection_customizer
    }
}
