# EctoPoolsConnectionCache

An Ecto pool that is more of an LRU connection cache than a connection pool.
It allows for connecting to multiple databases from a single repo and tries
to maintain target size, although it makes no guarantees.

## Why??

We have a heavily sharded database setup that unfortunately uses real Postgres
_databases_ for each shard, rather than a schema for each shard. Further, we
have multiple database _servers_ that these shards are spread across.

For example:
```
db01.company.com
  shard001
  ...
  shard200

db02.company.com
  shard201
  ...
  shard400

db03.company.com
  shard401
  ...
  shard600
```

So that's 600 databases spread across 3 servers. If we use normal Ecto repos
and connection pools, that means 600 repos each with a pool of, say, 20
connections. And if we have 100 web servers and background workers, that makes
for `600 * 20 * 100 = 1_200_000` connections to Postgres. Thats too much,
even for pgBouncer.

Further, those shards are created dynamically and we want our app to be able
to use them without deploying code or restarting.

## Installation

  1. Add `ecto_pools_connection_cache` to your list of dependencies in `mix.exs`:

    ```elixir
    def deps do
      [{:ecto_pools_connection_cache, github: "cjbottaro/ecto_pools_connection_cache"}]
    end
    ```

  2. Configure an Ecto repo in `config.exs` (or some such):

    ```elixir
    config :your_app, YourRepo,
      adapter: Ecto.Adapters.PostgresWithoutCache,
      pool: Ecto.Pools.ConnectionCache,
      prepare: :unnamed,
      pool_size: 20,
      database: "your_app_dev"
    ```

  3. Setup your repo module:

    ```elixir
    defmodule YourRepo do
      # Instead of `use Ecto.Repo, otp_app: :your_app`, do this.
      # It calls the aforementioned code, but also sets up some other stuff.
      use Ecto.Repo.ConnectionCache, otp_app: :your_app
    end
    ```

Note that it requires a special adapter to work (one that disables query
caching). You can get one for Postgres here:

https://github.com/cjbottaro/ecto_adapters_postgres_without_cache

## Usage

Use your repo like normal and connections will be made as needed (lazily).
If more connections are needed than `pool_size` they _will be made_, but
connection pruning will be attempted on each checkin.

The interesting part is when you want to connect to a different database:

```elixir
YourRepo.set_database(host: "db02.company.com", database: "shard02")
Repo.get(SomeModel, 1) # Will use a new connection to above database.
YourRepo.set_database(host: "db03.company.com", database: "shard10")
Repo.get(SomeModel, 1) # Will use a new connection to above database.
YourRepo.set_database(host: "db02.company.com", database: "shard02")
Repo.get(SomeModel, 1) # Should used a cached connection.
```

The connections are cached in LRU fashion.

## Default database

If you use the repo without calling `set_database/1` first, then the _default
database_ is used, which is what is specified in `config.exs`.

You can switch back to the default connection with:

```elixir
YourRepo.set_database(nil)
# OR
YourRepo.set_database(:default)
```

## set_database/1

It is very important to note that the options passed to `set_database/1` are
_merged_ into the default database options. So if your `config.exs` looks like:

```elixir
config :your_app, YourRepo,
  host: "localhost",
  database: "your_app_dev",
  port: 5432
```

And then you say:

```elixir
YourRepo.set_database(database: "blah")
```

The connection made to the database will use these options:

```elixir
{ host: "localhost",
  database: "blah",
  port: 5432 }
```

`set_database/1` can accept a plain string as a convenience:

```elixir
YourRepo.set_database("foo")
# is convenience for
YourRepo.set_database(database: "foo")
```

`set_database/1` configures the connection _globally_ for the _current process_.
If you start a new process, then you need to call `set_database/1` in that
process. If you do not, then the _default database_ will be used in that
process.
