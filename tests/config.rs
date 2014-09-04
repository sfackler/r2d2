use std::default::Default;

use r2d2;

#[test]
fn default_ok() {
    let config: r2d2::Config = Default::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_zero_initial_size_err() {
    let config = r2d2::Config {
        initial_size: 0,
        ..Default::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn test_zero_helper_tasks_err() {
    let config = r2d2::Config {
        helper_tasks: 0,
        ..Default::default()
    };
    assert!(config.validate().is_err());
}
