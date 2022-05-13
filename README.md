# Mammoth 🐘 → ❄️

*Synchronize Postgres to Snowflake in near-real time*

Mammoth uses the Postgres logical replication to obtain a stream of changes and writes micro-batches to Snowflake. It
has no additional dependencies.

## Features

* **Simple.** Point it at Postgres and Snowflake, wait, everything in Postgres shows up in Snowflake and
  stays up-to-date.
* **Near-real time.** Changes from Postgres are micro-batched for efficiency and written at a configurable interval. <
  60 seconds is reasonable.
* **Hands off.** Generates tables in Snowflake based on the schema in Postgres. Handles schema
  changes.
* **Operationally easy.** Switches from table creation, initial back-fill, and streaming synchronization as needed.
  All state is in Postgres and Snowflake: no additional data stores or services required.

Tested with Postgres 10 (may work for 11+, see Roadmap)

## Quick Start

Make sure that logical replication is enabled in your Postgres instance (see Usage for details).

### Docker

Create a `config.yml` file specifying which tables to replicate:

```yaml
sync:
  tables:
    - public.table1
    - public.table2
```

Run the container. Environment variables configure the Postgres and Snowflake connections and mount the config 
file at `/app/config.yml`:

```
docker run --rm -v $PWD/config.yml:/app/config.yml \
-e POSTGRES_CONNECTION=postgres://user:password@localhost/db \
-e SNOWFLAKE_CONNECTION=user:password@account/my_database/my_schema \
samjbobb/mammoth
```

### From Source

[Install Go](https://golang.org/dl/)

Clone this repo and build:

```
go build -o mammoth ./cmd/mammoth
```

Create `config.yml` as above in the same directory and run:

```
SNOWFLAKE_CONNECTION=user:password@account/my_database/my_schema \
POSTGRES_CONNECTION=postgres://user:password@localhost/db ./mammoth
```

## Usage

Enable Postgres logical replication by setting the following in your Postgres configuration
(`postgresql.conf`)

```
wal_level = logical
max_replication_slots = 10 // must be greater than 0, 10 is the default for recent Postgres versions
```

Restart Postgres after changing the `wal_level`. Check the change by running `show wal_level;`

Create a target schema in Snowflake for Mammoth to write to:

```postgresql
CREATE DATABASE my_database;
CREATE SCHEMA my_database.my_schema;
```

`mammoth` is configured with a `config.yml` file. `default-config.yml` shows a reference for the default values.
You only need to set parameters that you want to override. Each config parameter can also be set with an environment
variable, where levels in the config hierarchy are separated by `_` in the environment variable.

At a minimum you need to configure:

* Postgres connection
* Snowflake connection
* The tables to sync

As an example, we'll sync the tables `public.table1` and `public.table2` and configure the connection strings using
environment variables.

Create a `config.yml` file with the following contents:

```yaml
sync:
  tables:
    - public.table1
    - public.table2
```

You must specify every table you want to replicate in Snowflake. Each table you replicate must have a PRIMARY KEY.

Then run:

```
docker run --rm -v $PWD/config.yml:/app/config.yml \
-e POSTGRES_CONNECTION=postgres://user:password@localhost/db \
-e SNOWFLAKE_CONNECTION=user:password@account/my_database/my_schema \
samjbobb/mammoth
```

Run `docker run --rm samjbobb/mammoth --help` for a list of commands and options.

### ⚠️ Remember to remove unused replication slots ⚠️

Mammoth creates a replication slot in your Postgres database. While running, Mammoth uses this slot to stream
changes from the database. This places very little additional load on the database. However,
a replication
slot without an active consumer causes Postgres
to save a log of all changes in the database indefinitely. This will eventually cause PG to fill up all available disk
space which will make Postgres go offline. So, when
you're done, delete the replication slot created by Mammoth.

List slots:

```postgresql
select * from pg_replication_slots;
```

Drop a slot by name:

```postgresql
select pg_drop_replication_slot('mammoth');
```

## Development

Pull this repo and run as described above.

### Running Tests

Tests require the following environment variables:

```
POSTGRES_CONNECTION=postgres://username:password@host/database
SNOWFLAKE_CONNECTION=username:password@account/database
```

Replace `username`, `password`, `host`, `database`, and `account` with real values. A database must be created in
Postgres and Snowflake before running tests (the name does not have to be the same in Postgres and Snowflake).

Run:

`go test -v -race  ./...`

## Comparison to other solutions

TODO: add comparison for the following tools

* [Debezium](https://debezium.io)

* [Meltano](https://meltano.com), [PipelineWise](https://transferwise.github.io/pipelinewise/), and other Singer tools

* [Stitch](https://www.stitchdata.com), [Fivetran](https://www.fivetran.com), and other hosted ETL/ELT tools

* [AWS Data Migration Service](https://aws.amazon.com/dms/)

* [ihippik/wal-listener](https://github.com/ihippik/wal-listener)

* [hasura/pgdeltastream](https://github.com/hasura/pgdeltastream)



## When not to use this

### When you need an event stream

Don't use this if you actually need a multi-writer, multi-reader event stream (Kafka, NATS, Redpanda). Event streams
allow high decoupling of the source and destination datastores. Data sources can write to the event
stream with minimal regard for which consumer care about a particular message. This decoupling is great when there
are separate teams, code bases, or deployments managing sources and destinations. It's necessary for the
microservice dream.

Mammoth does not create this architecture. Instead, the target (Snowflake) becomes very coupled to the
source (Postgres). They're so coupled in-fact that you should think of them as a single data store. Migrations run
on Postgres will automatically propagate through Mammoth to Snowflake. Your code that writes to Postgres should
probably live with the code that reads from Snowflake.

Think about event streams before you choose Mammoth. See the excellent post [The
Log: What every software engineer should know about real-time data's unifying abstraction](https://engineering.
linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) and
the great book
[Designing Data Intensive Applications](https://www.amazon.
com/Designing-Data-Intensive-Applications-Reliable-Maintainable/dp/1449373321).

Consider using Mammoth when the complexity of this decoupling and running additional stream services seems unnecessary.

### When you have multiple sources or destinations

Mammoth syncs one Postgres instance to one destination. An additional Mammoth service will be needed for each pair
source and destinations.

### When you're happy hourly batches

Mammoth using the Postgres logical replication feature and puts special effort into syncing data to Snowflake in
"near realtime" (roughly <60s). If you don't need this feature, consider a traditional ETL tool like Meltano or
Fivetran.

### When you only control the Snowflake side

From the perspective of a typical analytics task, Postgres side is out of your hands: you work in Snowflake only. In
that case, automatically migrating Snowflake schema based on Postgres changes may not be what you want.

### When tables to be synced do not have a primary key

A primary key is currently required on each table replicated. 

## How it works

![Flow chat describing the program logic](flow-diagram.png)

Mammoth does not reject any Postgres types. Many Postgres types are replicated in Snowflake with the corresponding
Snowflake type (integers, timestamps, etc.). Unknown or custom Postgres types are replicated as
TEXT with the Postgres text representation. See [getSfType](/target/snowflake/snowflake_target.go#L675) for complete
mapping of Postgres types to Snowflake types.

## Known issues and roadmap

- [ ] Tests to verify Postgres output matches expectations (`stream_test.go`)
- [ ] Tests to cover all Postgres types
- [ ] Test with Postgres 11, 12, 13, 14
- [ ] Add support for TRUNCATE replication (added in Postgres 11)
- [ ] Benchmarks throughput
- [ ] Monitoring and introspection (buffers full? are writes in progress? is syncing complete to X position / XID?
  when can I read my own writes?)
- [ ] Handle table schema changes without full reload
- [ ] Redshift support
- [ ] BigQuery support

## Contributions

Contributions are welcome and encouraged. Open a PR. For larger changes, open an Issue first to discuss the approach.
