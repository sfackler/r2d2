# r2d2
[![CircleCI](https://circleci.com/gh/sfackler/r2d2.svg?style=sheild)](https://circleci.com/gh/sfackler/r2d2)

A generic connection pool for Rust.

[Documentation](https://docs.rs/r2d2)

Opening a new database connection every time one is needed is both inefficient
and can lead to resource exhaustion under high traffic conditions. A connection
pool maintains a set of open connections to a database, handing them out for
repeated use.

r2d2 is agnostic to the connection type it is managing. Implementors of the
`ManageConnection` trait provide the database-specific logic to create and
check the health of connections.

A (possibly not exhaustive) list of adaptors for different backends:

Backend                                                             | Adaptor Crate
------------------------------------------------------------------- | -------------
[rust-postgres](https://github.com/sfackler/rust-postgres)          | [r2d2-postgres](https://github.com/sfackler/r2d2-postgres)
[redis-rs](https://github.com/mitsuhiko/redis-rs)                   | [r2d2-redis](https://github.com/nevdelap/r2d2-redis)
[rust-mysql-simple](https://github.com/blackbeam/rust-mysql-simple) | [r2d2-mysql](https://github.com/outersky/r2d2-mysql)
[rusqlite](https://github.com/jgallagher/rusqlite)                  | [r2d2-sqlite](https://github.com/ivanceras/r2d2-sqlite)
[rusted-cypher](https://github.com/livioribeiro/rusted-cypher)      | [r2d2-cypher](https://github.com/flosse/r2d2-cypher)
[diesel](https://github.com/sgrif/diesel)                           | [r2d2-diesel](https://github.com/sgrif/r2d2-diesel)
[couchdb](https://github.com/chill-rs/chill)                        | [r2d2-couchdb](https://github.com/scorphus/r2d2-couchdb)

# Example

Using an imaginary "foodb" database.
```rust
use std::thread;

extern crate r2d2;
extern crate r2d2_foodb;

fn main() {
    let config = r2d2::Config::builder()
        .pool_size(15)
        .build();
    let manager = r2d2_foodb::FooConnectionManager::new("localhost:1234");

    let pool = r2d2::Pool::new(config, manager).unwrap();

    for _ in 0..20 {
        let pool = pool.clone();
        thread::spawn(move || {
            let conn = pool.get().unwrap();
            // use the connection
            // it will be returned to the pool when it falls out of scope.
        })
    }
}
```

## License

Licensed under either of
 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be dual licensed as above, without any
additional terms or conditions.
