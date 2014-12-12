use std::default::Default;
use std::fmt;
use std::error::Error;

/// A struct specifying the runtime configuration of a pool.
///
/// `Config` implements `Default`, which provides a set of reasonable default
/// values.
#[deriving(Encodable, Decodable, Copy, Clone)]
pub struct Config {
    /// The number of connections managed by the pool.
    ///
    /// Must be positive.
    ///
    /// Defaults to 10.
    pub pool_size: uint,
    /// The number of tasks that the pool will use for asynchronous operations
    /// such as connection creation and health checks.
    ///
    /// Must be positive.
    ///
    /// Defaults to 3.
    pub helper_tasks: uint,
    /// If true, the health of a connection will be verified via a call to
    /// `PoolManager::is_valid` before it is checked out of the pool.
    ///
    /// Defaults to false.
    pub test_on_check_out: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            pool_size: 10,
            helper_tasks: 3,
            test_on_check_out: false,
        }
    }
}

impl Config {
    /// Determines if the configuration is valid
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.pool_size == 0 {
            return Err(ConfigError::ZeroPoolSize);
        }

        if self.helper_tasks == 0 {
            return Err(ConfigError::ZeroHelperTasks);
        }

        Ok(())
    }
}

/// An enumeration of reasons that a `Config` is invalid
#[deriving(Copy, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// pool_size was zero
    ZeroPoolSize,
    /// helper_tasks was zero
    ZeroHelperTasks,
}

impl fmt::Show for ConfigError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.description())
    }
}

impl Error for ConfigError {
    fn description(&self) -> &str {
        match *self {
            ConfigError::ZeroPoolSize => "pool_size must be positive",
            ConfigError::ZeroHelperTasks => "helper_tasks must be positive",
        }
    }
}
