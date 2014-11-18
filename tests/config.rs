use std::default::Default;

use r2d2;

#[test]
fn default_ok() {
    let config: r2d2::Config = Default::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_zero_pool_size_err() {
    let config = r2d2::Config {
        pool_size: 0,
        ..Default::default()
    };
    assert_eq!(Err(r2d2::ConfigError::ZeroPoolSize), config.validate());
}

#[test]
fn test_zero_helper_tasks_err() {
    let config = r2d2::Config {
        helper_tasks: 0,
        ..Default::default()
    };
    assert_eq!(Err(r2d2::ConfigError::ZeroHelperTasks), config.validate());
}
