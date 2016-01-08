//! Pool configuration.
use std::fmt;
use std::time::Duration;
use std::error::Error;

use {HandleError, LoggingErrorHandler, CustomizeConnection, NopConnectionCustomizer};

/// A builder for `Config`.
///
/// See the documentation of `Config` for more details about the default value
/// and meaning of the configuration parameters.
#[derive(Debug)]
pub struct Builder<C, E> {
    c: Config<C, E>,
}

impl<C, E: Error> Builder<C, E> {
    /// Constructs a new `Builder`.
    ///
    /// Parameters are initialized with their default values.
    pub fn new() -> Builder<C, E> {
        Builder { c: Config::default() }
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

    /// Sets `min_idle`.
    pub fn min_idle(mut self, min_idle: Option<u32>) -> Builder<C, E> {
        self.c.min_idle = min_idle;
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

    /// Sets `max_lifetime`.
    ///
    /// # Panics
    ///
    /// Panics if `max_lifetime` is the zero `Duration`.
    pub fn max_lifetime(mut self, max_lifetime: Option<Duration>) -> Builder<C, E> {
        assert!(max_lifetime != Some(Duration::from_secs(0)),
                "max_lifetime must be positive");
        self.c.max_lifetime = max_lifetime;
        self
    }

    /// Sets `idle_timeout`.
    ///
    /// # Panics
    ///
    /// Panics if `idle_timeout` is the zero `Duration`.
    pub fn idle_timeout(mut self, idle_timeout: Option<Duration>) -> Builder<C, E> {
        assert!(idle_timeout != Some(Duration::from_secs(0)),
                "idle_timeout must be positive");
        self.c.idle_timeout = idle_timeout;
        self
    }

    /// Sets `connection_timeout` to the specified duration.
    ///
    /// # Panics
    ///
    /// Panics if `connection_timeout` is the zero duration
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> Builder<C, E> {
        assert!(connection_timeout > Duration::from_secs(0),
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
    pub fn connection_customizer(mut self,
                                 connection_customizer: Box<CustomizeConnection<C, E>>)
                                 -> Builder<C, E> {
        self.c.connection_customizer = connection_customizer;
        self
    }

    /// Consumes the `Builder`, turning it into a `Config`.
    ///
    /// # Panics
    ///
    /// Panics if `min_idle` is larger than `pool_size`.
    pub fn build(self) -> Config<C, E> {
        if let Some(min_idle) = self.c.min_idle {
            assert!(self.c.pool_size >= min_idle,
                    "min_idle must be no larger than pool_size");
        }

        self.c
    }
}

/// A struct specifying the runtime configuration of a pool.
///
/// `Config` implements `Default`, which provides a set of reasonable default
/// values. It can be constructed using a `Builder`.
pub struct Config<C, E> {
    pool_size: u32,
    min_idle: Option<u32>,
    helper_threads: u32,
    test_on_check_out: bool,
    initialization_fail_fast: bool,
    max_lifetime: Option<Duration>,
    idle_timeout: Option<Duration>,
    connection_timeout: Duration,
    error_handler: Box<HandleError<E>>,
    connection_customizer: Box<CustomizeConnection<C, E>>,
}

impl<C, E> fmt::Debug for Config<C, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Config")
           .field("pool_size", &self.pool_size)
           .field("min_idle", &self.min_idle)
           .field("helper_threads", &self.helper_threads)
           .field("test_on_check_out", &self.test_on_check_out)
           .field("initialization_fail_fast", &self.initialization_fail_fast)
           .field("max_lifetime", &self.max_lifetime)
           .field("idle_timeout", &self.idle_timeout)
           .field("connection_timeout", &self.connection_timeout)
           .finish()
    }
}

impl<C, E: Error> Default for Config<C, E> {
    fn default() -> Config<C, E> {
        Config {
            pool_size: 10,
            min_idle: None,
            helper_threads: 3,
            test_on_check_out: true,
            initialization_fail_fast: true,
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            connection_timeout: Duration::from_secs(30),
            error_handler: Box::new(LoggingErrorHandler),
            connection_customizer: Box::new(NopConnectionCustomizer),
        }
    }
}

impl<C, E: Error> Config<C, E> {
    /// Creates a new `Builder` which can be used to construct a customized
    /// `Config`.
    ///
    /// All parameters are initialized to their default values.
    pub fn builder() -> Builder<C, E> {
        Builder::new()
    }

    /// The maximum number of connections managed by the pool.
    ///
    /// Defaults to 10.
    pub fn pool_size(&self) -> u32 {
        self.pool_size
    }

    /// If set, the pool will try to maintain at least this many idle
    /// connections at all times, while respecting the value of `pool_size`.
    ///
    /// Defaults to None (equivalent to the value of `pool_size`).
    pub fn min_idle(&self) -> Option<u32> {
        self.min_idle
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

    /// If set, connections will be closed after sitting idle for at most 30
    /// seconds beyond this duration.
    ///
    /// Defaults to 10 minutes.
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    /// If set, connections will be closed after existing for at most 30 seconds
    /// beyond this duration. If a connection reaches its maximum lifetime while
    /// checked out it will be closed when it is returned to the pool.
    ///
    /// Defaults to 30 minutes.
    pub fn max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime
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
    /// Defaults to `r2d2::LoggingErrorHandler`.
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use test::Error;

    #[test]
    fn builder() {
        let config = Config::<(), Error>::builder()
                         .pool_size(1)
                         .helper_threads(2)
                         .test_on_check_out(false)
                         .initialization_fail_fast(false)
                         .connection_timeout_ms(3 * 1000)
                         .build();
        assert_eq!(1, config.pool_size());
        assert_eq!(2, config.helper_threads());
        assert_eq!(false, config.test_on_check_out());
        assert_eq!(false, config.initialization_fail_fast());
        assert_eq!(3 * 1000, config.connection_timeout_ms());
    }

    #[test]
    #[should_panic(expected = "pool_size must be positive")]
    fn builder_zero_pool_size() {
        Config::<(), Error>::builder().pool_size(0);
    }

    #[test]
    #[should_panic(expected = "helper_threads must be positive")]
    fn builder_zero_helper_threads() {
        Config::<(), Error>::builder().helper_threads(0);
    }

    #[test]
    #[should_panic(expected = "connection_timeout must be positive")]
    fn builder_zero_connection_timeout() {
        Config::<(), Error>::builder().connection_timeout(Duration::from_secs(0));
    }

    #[test]
    #[should_panic(expected = "idle_timeout must be positive")]
    fn builder_zero_idle_timeout() {
        Config::<(), Error>::builder().idle_timeout(Some(Duration::from_secs(0)));
    }

    #[test]
    #[should_panic(expected = "max_lifetime must be positive")]
    fn builder_zero_max_lifetime() {
        Config::<(), Error>::builder().max_lifetime(Some(Duration::from_secs(0)));
    }

    #[test]
    #[should_panic(expected = "min_idle must be no larger than pool_size")]
    fn builder_too_many_num_idle() {
        Config::<(), Error>::builder().pool_size(1).min_idle(Some(2)).build();
    }
}
