# Change Log

## [Unreleased]

## [0.8.1] - 2017-10-28

### Fixed

* Fixed the example in the README.

## [0.8.0] - 2017-10-26

### Changed

* Pool configuration has changed. Rather than constructing a `Config` and passing it to the `Pool`
    constructor, you now configure a `Builder` which then directly constructs the pool:

    ```rust
    // In 0.7.x
    let config = Config::builder()
        .min_idle(3)
        .build();
    let pool = Pool::new(config, manager)?;

    // In 0.8.x
    let pool = Pool::builder()
        .min_idle(3)
        .build(manager)?;
    ```

* The `Pool::new` method can be used to construct a `Pool` with default settings:

    ```rust
    // In 0.7.x
    let config = Config::default();
    let pool = Pool::new(config, manager)?;

    // In 0.8.x
    let pool = Pool::new(manager)?;
    ```

* The `initialization_fail_fast` configuration option has been replaced with separate
    `Builder::build` and `Builder::build_unchecked` methods. The second returns a `Pool` directly
    without wrapping it in a `Result`, and does not check that connections are being successfully
    opened:

    ```rust
    // In 0.7.x
    let config = Config::builder()
        .initialization_fail_fast(false)
        .build();
    let pool = Pool::new(config, manager).unwrap();

    // In 0.8.x
    let pool = Pool::builder().build_unchecked(manager);
    ```

* The `InitializationError` and `GetTimeout` error types have been merged into a unified `Error`
    type.

* The `Pool::config` method has been replaced with accessor methods on `Pool` to directly access
    configuration, such as `Pool::min_idle`.

### Removed

* The deprecated `Builder::num_threads` method has been removed. Construct a `ScheduledThreadPool`
    and set it via `Builder::thread_pool` instead.

## Older

Look at the [release tags] for information about older releases.

[Unreleased]: https://github.com/sfackler/r2d2/compare/v0.8.1...HEAD
[0.8.1]: https://github.com/sfackler/r2d2/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/sfackler/r2d2/compare/v0.7.4...v0.8.0
[release tags]: https://github.com/sfackler/r2d2/releases
