r2d2
====

A generic connection pool for Rust.

[![Build Status](https://travis-ci.org/sfackler/r2d2.svg?branch=master)](https://travis-ci.org/sfackler/r2d2) [![Latest Version](https://img.shields.io/crates/v/r2d2.svg)](https://crates.io/crates/r2d2)

[Documentation](https://sfackler.github.io/r2d2/doc/master/r2d2)

Opening a new database connection every time one is needed is both inefficient
and can lead to resource exhaustion under high traffic conditions. A connection
pool maintains a set of open connections to a database, handing them out for
repeated use.

r2d2 is agnostic to the connection type it is managing. Implementors of the
`ConnectionManager` trait provide the database-specific logic to create and
check the health of connections.

The [r2d2-postgres](https://github.com/sfackler/r2d2-postgres) crate provides
a `ConnectionManager` for [rust-postgres](https://github.com/sfackler/rust-postgres).

# Example

Using a fake "foodb" database.
```rust
use std::thread;
use std::sync::Arc;

extern crate r2d2;
extern crate r2d2_foodb;

fn main() {
    let config = r2d2::Config::default();
    let manager = r2d2_foodb::FooConnectionManager::new("localhost:1234");
    let error_handler = Box::new(r2d2::LoggingErrorHandler);

    let pool = Arc::new(r2d2::Pool::new(config, manager, error_handler).unwrap());

    for _ in 0..20i32 {
        let pool = pool.clone();
        thread::spawn(move || {
            let conn = pool.get().unwrap();
            // use the connection
            // it will be returned to the pool when it falls out of scope.
        })
    }
}
```
