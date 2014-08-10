use std::default::Default;

/// A struct specifying the runtime configuration of a pool.
///
/// `Config` implements `Default`, which provides a set of reasonable default
/// values.
pub struct Config {
    /// The number of connections that will be made during pool creation.
    ///
    /// Must be no greater than `max_size`.
    ///
    /// Defaults to 3.
    pub initial_size: uint,
    /// The maximum number of connections that the pool will maintain.
    ///
    /// Must be positive and no less than `initial_size`.
    ///
    /// Defaults to 15.
    pub max_size: uint,
    /// The number of connections that will be created at once when the pool is
    /// exhausted.
    ///
    /// Must be positive.
    ///
    /// Defaults to 3.
    pub acquire_increment: uint,
    /// The number of tasks that the pool will use for asynchronous operations
    /// such as connection creation and health checks.
    ///
    /// Must be positive.
    ///
    /// Defaults to 3.
    pub helper_tasks: uint,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            initial_size: 3,
            max_size: 15,
            acquire_increment: 3,
            helper_tasks: 3,
        }
    }
}

impl Config {
    /// Determines if the configuration is valid
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_size == 0 {
            return Err("max_size must be positive");
        }

        if self.initial_size > self.max_size {
            return Err("initial_size cannot be greater than max_size");
        }

        if self.acquire_increment == 0 {
            return Err("acquire_increment must be positive");
        }

        if self.helper_tasks == 0 {
            return Err("helper_tasks must be positive");
        }

        Ok(())
    }
}

