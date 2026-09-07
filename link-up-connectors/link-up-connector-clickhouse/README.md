# Link-Up ClickHouse Connector

`link-up-connector-clickhouse` provides standalone bounded/offline ClickHouse Source and Sink implementations.

The connector keeps ClickHouse-specific planning and write semantics inside the module while reusing the official ClickHouse Java/JDBC client over HTTP.

## Source — Stage 1

The bounded Source keeps split planning at the physical-part level, which is the unit ClickHouse itself uses for scans:

```text
ClickHouseSource
  -> schema discovery through the official ClickHouse JDBC/HTTP driver
  -> table mode: system.tables + system.parts(active = 1)
  -> deterministic active-part splits
  -> split-bound ClickHouse HTTP/JDBC query
  -> ResultSet
  -> FluxRow
```

Supported:

- bounded/offline reads only
- single-table and `table_list` multi-table configuration
- schema discovery from `system.columns`
- primary-key metadata from `system.columns.is_in_primary_key`
- MergeTree-family table reads split by active parts from `system.parts`
- `partition_list` filtering during part discovery
- `split.size` / `split_size` part grouping
- `filter_query` ClickHouse-side filtering
- `batch_size` reader batching
- SQL mode for bounded custom queries
- Distributed table mode through one ClickHouse distributed query
- multiple HTTP nodes for local MergeTree shard reads
- `clickhouse.config` passthrough to the official JDBC client
- explicit `server_time_zone`
- conservative scalar type mapping with exact unsigned/high-width ranges

For a local MergeTree table with multiple `host` entries, configure one reachable node per shard. Do not list multiple replicas containing the same local data as independent hosts, otherwise the same physical parts can be read more than once.

For a ClickHouse `Distributed` table, Stage 1 creates one bounded distributed query and lets ClickHouse execute the cluster fan-out. Custom SQL is also one bounded Link-Up split in this stage; JOIN/GROUP BY/subquery shard rewriting is intentionally not attempted.

### Source example

```hocon
source {
  ClickHouse {
    host = "ch-shard-1:8123,ch-shard-2:8123"
    username = "default"
    password = ""

    table_path = "analytics.orders"
    partition_list = ["20260905", "20260906"]
    filter_query = "status = 'PAID'"
    split.size = 2
    batch_size = 1024
    server_time_zone = "UTC"

    clickhouse.config = {
      socket_timeout = "300000"
    }
  }
}
```

### Source type boundary

The bounded Source maps scalar ClickHouse types conservatively:

- `Bool` -> `BOOLEAN`
- `Int8` -> `TINYINT`
- `UInt8` / `Int16` -> `SMALLINT`
- `UInt16` / `Int32` -> `INT`
- `UInt32` / `Int64` -> `BIGINT`
- `UInt64` -> `DECIMAL(20,0)`
- `Int128` / `UInt128` / `Int256` / `UInt256` -> exact `STRING`
- `Float32` -> `FLOAT`
- `Float64` -> `DOUBLE`
- `Decimal*` -> `DECIMAL`
- `Date` / `Date32` -> `DATE`
- `DateTime` / `DateTime64` -> `TIMESTAMP`
- string/UUID/IP/Enum/JSON/Dynamic/Variant/geometric scalar-like values -> `STRING`
- `Interval*` -> `BIGINT`

`Nullable`, `LowCardinality`, and scalar `SimpleAggregateFunction` wrappers are unwrapped while preserving nullability/source type metadata. `ARRAY`, `MAP`, `TUPLE`, `NESTED`, and `AggregateFunction` remain fail-fast until dedicated Flux conversion semantics exist.

## Sink — Stage 2

The Sink is intentionally a bounded client-side batch writer:

```text
FluxRow
  -> prepared target metadata
  -> PreparedStatement.addBatch()
  -> row threshold
  -> executeBatch()
  -> ClickHouse synchronous INSERT acknowledgement
```

The implementation follows ClickHouse's own recommendation to avoid small one-row inserts. Link-Up batches rows on the client and uses the official JDBC prepared batch path rather than inventing a custom transport.

### Stage 2 semantics

- exactly one target table per Sink task
- exactly one configured write endpoint
- target table must already exist
- target metadata is discovered from `system.columns` before writers start
- source/target field names must match; target physical order may differ
- nullable source fields cannot target non-nullable columns
- integer widening is allowed only when it is range-safe
- Decimal target precision/scale must fully contain the source Decimal
- default `sink.batch_size` is `10000`
- flush happens at the configured row threshold and at `prepareCommit()`
- each successful `executeBatch()` is already a remote ClickHouse write
- `commit()` is therefore a Link-Up task lifecycle boundary, not a second database transaction
- `abort()` only clears the not-yet-sent JDBC batch
- `close()` does **not** flush implicitly
- ambiguous `executeBatch()` failures are not retried automatically because replay can duplicate rows

ClickHouse 26.3+ can enable asynchronous inserts by default. This bounded Sink deliberately pins its write connection to `async_insert=0` and `wait_for_async_insert=1`, so a successful `executeBatch()` remains a stable remote durability boundary independent of cluster/user defaults. A conflicting `clickhouse.config` is rejected during configuration parsing.

### Sink example

```hocon
sink {
  ClickHouse {
    host = "ch-write:8123"
    username = "default"
    password = ""
    database = "analytics"
    table = "orders"

    sink.batch_size = 10000
    server_time_zone = "UTC"

    clickhouse.config = {
      socket_timeout = "300000"
    }
  }
}
```

Use one load-balancer endpoint, one `Distributed` table endpoint, or one explicit ClickHouse node. Stage 2 does not round-robin writes across a comma-separated host list because a network failure after an ambiguous write acknowledgement cannot be safely replayed against another node without stronger target-side idempotency semantics.

### Sink type boundary

The bounded Sink accepts Flux scalar values with explicit JDBC binding for:

- `BOOLEAN`
- `TINYINT`
- `SMALLINT`
- `INT`
- `BIGINT`
- `FLOAT`
- `DOUBLE`
- `DECIMAL`
- `STRING`
- `DATE`
- `TIMESTAMP`

`TIMESTAMP` is bound as `LocalDateTime` instead of being forced through `java.sql.Timestamp`, avoiding an extra timezone reinterpretation for ClickHouse `DateTime64` paths.

The current Stage 2 Sink fails before or during binding for `BYTES`, standalone `TIME`, `TIMESTAMP_TZ`, `ARRAY`, `MAP`, and `ROW`. Those need explicit ClickHouse target semantics instead of implicit stringification or timezone conversion.

## Explicit non-goals

The ClickHouse connector remains an offline/bounded connector. These capabilities are outside the current stages:

- CDC / streaming / continuous polling
- mutation-log or Keeper-based change capture
- exactly-once streaming checkpoint semantics
- job-level transaction/XA semantics
- INSERT replay retries after ambiguous network failures
- ReplacingMergeTree interpreted as a generic UPSERT API
- DELETE / UPDATE / mutation semantics
- runtime schema evolution
- automatic target table creation in the Sink
- automatic Distributed-table-to-local-table SQL rewriting
- automatic JOIN/GROUP BY/subquery shard parallelization
- complex ARRAY/MAP/TUPLE/NESTED conversion

## Operational notes

- Source table mode requires a MergeTree-family or `Distributed` table. For other engines, configure an explicit bounded `sql` query.
- Empty MergeTree source tables simply enumerate zero splits and finish successfully.
- Source part names and filters are pushed to ClickHouse; Link-Up does not discard rows locally after a full-table read.
- Source Reader connections are split-scoped and closed on split completion/failure.
- Sink target validation is preparation-time only; runtime schema changes are rejected.
- A successful Sink flush cannot be rolled back by a later Link-Up task abort.
- If a whole Sink task is retried after some successful batches, verify target data first or rely on explicit target-side deduplication/key semantics. The connector does not claim job-level exactly once.
