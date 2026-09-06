# Link-Up ClickHouse Connector

`link-up-connector-clickhouse` provides a standalone bounded/offline ClickHouse Source.

The connector follows the mature read model used by Apache SeaTunnel while keeping the first Link-Up stage deliberately small:

```text
ClickHouseSource
  -> schema discovery through the official ClickHouse JDBC/HTTP driver
  -> table mode: system.tables + system.parts(active = 1)
  -> deterministic active-part splits
  -> split-bound ClickHouse HTTP/JDBC query
  -> ResultSet
  -> FluxRow
```

The transport is not reimplemented by Link-Up. Connections and query decoding use ClickHouse's official Java/JDBC client over HTTP. The ClickHouse-specific part of this connector is the bounded planning model: table mode understands MergeTree active parts and turns them into Link-Up `SourceSplit`s instead of treating ClickHouse as a generic unsplittable JDBC table.

## Stage 1 boundary

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

Explicitly out of scope:

- CDC / streaming / continuous polling
- mutation-log or Keeper-based change capture
- exactly-once streaming checkpoint semantics
- automatic SQL rewrite for JOIN/GROUP BY/subquery parallelization
- automatic Distributed-table-to-local-table SQL rewriting
- ARRAY / MAP / TUPLE / NESTED native conversion
- AggregateFunction state decoding
- runtime schema evolution
- ClickHouse Sink

## Why part-level splits

For a MergeTree-family table, ClickHouse stores current data in active data parts. `system.parts` exposes those parts and the `active` flag. SeaTunnel's ClickHouse Source uses the same model for table-mode parallel reads: parts are discovered per shard, grouped into source splits, and each split reads only its assigned `_part` values.

Link-Up keeps that model because it maps directly to `SourceSplitEnumerator` / `SourceReader` and avoids OFFSET-based partitioning.

For a local MergeTree table with multiple `host` entries, **configure one reachable node per shard**. Do not list multiple replicas containing the same local data as independent hosts, otherwise each replica would legitimately expose the same active parts and the job could read duplicate rows.

For a ClickHouse `Distributed` table this stage intentionally does not enumerate every replica. It creates one bounded table-query split and lets ClickHouse execute the distributed query itself.

## Single-table example

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

`split.size` means the maximum number of active ClickHouse parts grouped into one Link-Up split. Its default is `Integer.MAX_VALUE`, matching SeaTunnel's bounded ClickHouse Source default. `batch_size` defaults to `1024`.

## Multi-table example

```hocon
source {
  ClickHouse {
    host = "ch-1:8123"
    username = "default"
    password = ""

    table_list = [
      {
        table_path = "analytics.orders"
        filter_query = "id >= 100"
        split_size = 1
        batch_size = 2048
      },
      {
        table_path = "crm.customers"
        partition_list = ["202609"]
      }
    ]
  }
}
```

Inside `table_list`, use `split_size`; the flattened single-table form also accepts the SeaTunnel-style `split.size`. Link-Up compatibility aliases such as `scan_filter`, `request_part_size`, and `scan_batch_rows` are accepted.

## SQL mode

```hocon
source {
  ClickHouse {
    host = "ch-1:8123"
    username = "default"
    password = ""

    table_path = "analytics.orders"
    sql = "SELECT id, amount FROM analytics.orders WHERE created_at >= today() - 1"
    filter_query = "amount > 0"
    batch_size = 1024
  }
}
```

When `sql` is present, Link-Up treats it as a bounded query and wraps it as a subquery before applying an optional additional `filter_query`. `table_path` is optional in SQL mode; when omitted, the connector creates a synthetic dataset identity for the query result.

Unlike SeaTunnel's later-stage SQL splitter, Stage 1 does **not** rewrite simple SQL onto individual cluster shards and does not try to classify JOIN/GROUP BY/subquery semantics. A custom SQL query is one Link-Up split. This is a deliberate correctness boundary, not a missing streaming feature.

`partition_list` is table-mode-only. In SQL mode, write the partition predicate explicitly in SQL.

## Type boundary

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
- `String`, `FixedString`, UUID/IP/Enum/JSON/Dynamic/Variant/geometric scalar-like values -> `STRING`
- `Interval*` -> `BIGINT`

`Nullable`, `LowCardinality`, and scalar `SimpleAggregateFunction` wrappers are unwrapped while preserving nullability/source type metadata.

`UInt64` is not narrowed into Java signed `long`, and 128/256-bit integers are not narrowed into an insufficient decimal precision. This follows the same data-correctness rule used by the Link-Up StarRocks/Doris native sources.

`ARRAY`, `MAP`, `TUPLE`, `NESTED`, and `AggregateFunction` fail during schema preparation in Stage 1 rather than being silently converted through `String.valueOf(...)`.

## Operational notes

- Table mode requires a MergeTree-family or `Distributed` table. For other engines, configure an explicit bounded `sql` query.
- Empty MergeTree tables simply enumerate zero splits and finish successfully.
- Part names and filters are pushed to ClickHouse; Link-Up does not read entire parts and discard rows locally.
- Reader connections are split-scoped and are closed on split completion/failure.
- `batch_size` controls the Link-Up read batch/fetch hint; it does not change ClickHouse table semantics.
