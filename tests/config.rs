use std::default::Default;

use {r2d2, OkManager};

#[test]
fn test_zero_max_size_err() {
    let config = r2d2::Config {
        initial_size: 0,
        max_size: 0,
        ..Default::default() };
    assert!(r2d2::Pool::new(config, OkManager).is_err());
}

#[test]
fn test_zero_max_size_ok() {
    let config = r2d2::Config {
        initial_size: 0,
        ..Default::default()
    };
    assert!(r2d2::Pool::new(config, OkManager).is_ok());
}

#[test]
fn test_inverted_initial_max_size_err() {
    let config = r2d2::Config {
        initial_size: 5,
        max_size: 4,
        ..Default::default()
    };
    assert!(r2d2::Pool::new(config, OkManager).is_err());
}

#[test]
fn test_zero_acquire_increment_err() {
    let config = r2d2::Config {
        acquire_increment: 0,
        ..Default::default()
    };
    assert!(r2d2::Pool::new(config, OkManager).is_err());
}
