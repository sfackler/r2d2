# Change Log

## [Unreleased]

## [0.8.9] - 2020-06-30

## Changed

* Upgraded `parking_lot`.

## [0.8.7] - 2019-11-25

## Changed

* Upgraded `parking_lot`.

## [0.8.6] - 2019-10-19

## Added

* Added the ability to associate arbitrary data with pooled connections.

## [0.8.5] - 2019-06-06

## Changed

* Upgraded `parking_lot`.

## [0.8.4] - 2019-04-01

### Added

* Added a `HandleEvent` trait used to listen for various events from the pool for monitoring
    purposes.

### Changed

* Switched from standard library synchronization primitives to `parking_lot`.

## [0.8.3] - 2018-11-03

### Fixed

* The set of idle connections is now treated as a stack rather than a queue. The old behavior
    interacted poorly with configurations that allowed the pool size to shrink when mostly idle.

## [0.8.2] - 2017-12-24

### Changed

* Upgraded from log 0.3 to 0.4.

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

* The `scheduled_thread_pool` crate has been upgraded from 0.1 to 0.2.

### Removed

* The deprecated `Builder::num_threads` method has been removed. Construct a `ScheduledThreadPool`
    and set it via `Builder::thread_pool` instead.

## Older

Look at the [release tags] for information about older releases.

[Unreleased]: https://github.com/sfackler/r2d2/compare/v0.8.9...HEAD
[0.8.9]: https://github.com/sfackler/r2d2/compare/v0.8.8...v0.8.9
[0.8.7]: https://github.com/sfackler/r2d2/compare/v0.8.6...v0.8.7
[0.8.6]: https://github.com/sfackler/r2d2/compare/v0.8.5...v0.8.6
[0.8.5]: https://github.com/sfackler/r2d2/compare/v0.8.4...v0.8.5
[0.8.4]: https://github.com/sfackler/r2d2/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/sfackler/r2d2/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/sfackler/r2d2/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/sfackler/r2d2/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/sfackler/r2d2/compare/v0.7.4...v0.8.0
[release tags]: https://github.com/sfackler/r2d2/releases
