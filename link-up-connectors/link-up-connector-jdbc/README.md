# JDBC connector

The `jdbc` source is bounded and can synchronize one or more tables in one offline job. It does not poll for changes or
implement CDC. Configure
`table_list` with table objects for a multi-table job; each emitted batch keeps its source table identity so the JDBC
sink can create and write the matching target table.

```hocon
source {
  type = "jdbc"
  url = "jdbc:mysql://localhost:3306/flux_test"
  table_list = [
    { table_path = "flux_test.user_info" },
    { table_path = "flux_test.orders" }
  ]
}

sink {
  type = "jdbc"
  url = "jdbc:mysql://localhost:3306/flux_test"
  # `table` is an alias for `table_path`.
  table = "sink_${schema_name}_${table_name}"
}
```

The sink expands `${table_name}` with the source table name. `${schema_name}`
uses the source schema; for MySQL's `database.table` paths it uses the database name. A fixed `table` value, such
as `public.sink_table`, writes all input to that target. If `table`/`table_path` is configured, Flux generates the
INSERT/UPSERT SQL for the resolved target and ignores `custom_sql`/`query`.

## TiDB

TiDB reuses MySQL Connector/J and the MySQL-compatible JDBC execution path, but it is exposed as a separate database
dialect. Because both MySQL and TiDB use the `jdbc:mysql://` URL scheme, TiDB must be selected explicitly instead of
being guessed from the URL:

```hocon
source {
  type = "jdbc"
  url = "jdbc:mysql://tidb:4000/app"
  driver = "com.mysql.cj.jdbc.Driver"
  dialect = "tidb"
  table_path = "app.orders"
}

sink {
  type = "jdbc"
  url = "jdbc:mysql://tidb:4000/archive"
  driver = "com.mysql.cj.jdbc.Driver"
  dialect = "tidb"
}
```

The Stage 1 TiDB adapter supports bounded single-table and multi-table reads, shared JDBC range/hash split planning,
INSERT/UPSERT and offline sink DDL. Target database resolution prefers the database bound in the TiDB JDBC URL so a
source database name is not accidentally reused by a cross-database sink.

Stage 1 intentionally does not advertise `DATABASE_SNAPSHOT`, TiCDC, TiKV/TiFlash native access, CDC, streaming
checkpoints or runtime schema evolution. Those capabilities require separate stages instead of changing the bounded JDBC
contract.

## GoldenDB

GoldenDB is exposed as the separate `goldendb` JDBC dialect while Stage 1 reuses the MySQL-compatible execution path and
MySQL Connector/J. Because MySQL, TiDB and GoldenDB can share the `jdbc:mysql://` URL scheme, GoldenDB must be selected
explicitly with `dialect = "goldendb"` instead of being inferred from the URL:

```hocon
source {
  type = "jdbc"
  url = "jdbc:mysql://goldendb:3306/app"
  driver = "com.mysql.cj.jdbc.Driver"
  dialect = "goldendb"
  table_path = "app.orders"
}

sink {
  type = "jdbc"
  url = "jdbc:mysql://goldendb:3306/archive"
  driver = "com.mysql.cj.jdbc.Driver"
  dialect = "goldendb"
}
```

The Stage 1 GoldenDB adapter supports bounded single-table and multi-table reads, MySQL-compatible automatic type
mapping, shared JDBC range/hash split planning, INSERT/UPSERT, batch writes and metadata discovery. Sink target database
resolution prefers the database in the target JDBC URL so source database metadata does not leak into a cross-database
write.

GoldenDB Stage 1 writes existing target tables only. Automatic database/table creation, table recreation, add-column
schema evolution and other distribution/sharding-sensitive DDL are intentionally blocked. Create the GoldenDB target
schema/table before the job; `CREATE_SCHEMA_WHEN_NOT_EXIST` remains safe for an existing table but fails clearly when the
target table is absent. CDC, streaming, GoldenDB-native bulk loading and coordinated distributed snapshots are also out
of scope for this bounded JDBC stage.

## GBase family

GBase is modeled as a database family, not as one generic JDBC dialect. The stable product identities are `gbase8c`,
`gbase8a` and `gbase8s`. A generic `gbase` dialect is intentionally not defined because the three products use different
JDBC protocols and database semantics.

The family scaffold only provides stable product metadata and family-level helpers. It deliberately does **not** register
`JdbcDialectFactory` implementations yet, so these identifiers do not advertise runtime support before their concrete
URL, driver, catalog, type-mapping and SQL behavior is implemented and tested.

Shared GBase code must remain product-neutral. Driver names, JDBC URL parsing, identifier rules, catalog behavior, type
mapping, row conversion, INSERT/UPSERT SQL and distribution/MPP behavior belong to the concrete product adapter unless at
least two completed adapters prove the behavior is genuinely common. The planned bounded/offline implementation order is:

1. GBase 8c Source, then existing-table Sink.
2. GBase 8a Source, then existing-table JDBC Sink; native/high-speed MPP loading is a later stage.
3. GBase 8s Source, then existing-table Sink.

CDC, compatibility-mode expansion, automatic distributed-table design and product-native bulk-loading paths are outside
this scaffold.

## SAP HANA

SAP HANA is exposed as the `hana` JDBC dialect and is auto-detected from `jdbc:sap://` URLs. Stage 1 is deliberately
source-only: it provides bounded reads, schema/table metadata discovery, custom SQL projection, common HANA type mapping
and the shared safe JDBC split planner. It does not enable HANA sink DDL, INSERT/UPSERT, CDC, SLT or streaming semantics.

```hocon
source {
  type = "jdbc"
  url = "jdbc:sap://hana:30013/?databaseName=HXE"
  driver = "com.sap.db.jdbc.Driver"
  schema = "SALES"
  table_path = "SALES.ORDERS"
}
```

HANA SQL identifiers use `schema.table`; the database/tenant is selected by the JDBC connection. Unquoted `table_path`
parts are normalized to HANA's uppercase identifier semantics, while quoted identifiers preserve case. The connector
`schema` option is applied as the JDBC `currentSchema` default unless the URL or explicit JDBC properties already set
`currentSchema`.

The Stage 1 type contract covers BOOLEAN, integer types, SMALLDECIMAL/DECIMAL, REAL/DOUBLE, VARCHAR/NVARCHAR and common
text/LOB types, DATE/TIME/SECONDDATE/TIMESTAMP, and binary/BLOB types. ARRAY and spatial `ST_POINT`/`ST_GEOMETRY` are
rejected explicitly instead of being silently coerced.

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `url` | yes | — | JDBC connection URL. |
| `table_path` | one of `table_path` or `table_list` | — | Source table path. |
| `table_list` | one of `table_path` or `table_list` | — | List of `{ table_path = "..." }` source-table objects. |
| `query` | no | — | SQL query to read for a single table; requires `table_path`. |
| `username` | no | empty | JDBC user name. |
| `password` | no | empty | JDBC password. |
| `driver` | no | — | JDBC driver class to load before connecting. |
| `fetch_size` | no | `1000` | JDBC fetch size used while reading. |
| `read_consistency` | no | `BEST_EFFORT` | Read consistency: `BEST_EFFORT`, `SINGLE_CONNECTION_SNAPSHOT`, or a dialect-provided `DATABASE_SNAPSHOT`. |

The result columns retain their query order and use JDBC column labels as the
`FluxRow` field names.

## Partitioned reads

`partition_column`, `partition_lower_bound`, `partition_upper_bound`, and `partition_num` split a table into
deterministic, non-overlapping ranges. Numeric columns use `FixedChunkSplitter`; fixed-width ASCII keys
use `AsciiStringRangeSplitter`. The final range is upper-bound inclusive, so no boundary row is lost. Partition bounds
are required deliberately: this bounded source does not issue an unbounded `MIN`/`MAX` analysis query during planning.

## Execution parallelism

Configure job-level reader concurrency outside the connector configuration:

```hocon
env {
  parallelism = 4
}
```

The launcher creates at most `parallelism` source readers and assigns every split to one reader exactly once. JDBC uses
this value while planning range splits, so an unspecified `partition_num` produces no more than this many chunks per
table. Batches from different tables or ranges can be read concurrently, while the local sink remains single-threaded
because the current `SinkWriter` owns one transactional JDBC connection and is not safe to share across writers.

### Read consistency

`BEST_EFFORT` is the default and preserves the existing parallel reader behavior. With more than one source reader,
separate JDBC connections can observe different database snapshots; the connector emits a preparation-time warning
without connection credentials or tokens.

`SINGLE_CONNECTION_SNAPSHOT` requires `env.parallelism = 1`. The reader configures its JDBC connection as read-only,
disables auto-commit, and requests repeatable-read isolation before reading. It is available only when the selected JDBC
dialect declares support.

`DATABASE_SNAPSHOT` is reserved for dialects that can coordinate one database snapshot across multiple readers. The
built-in MySQL dialect does not implement it yet, so preparation fails before any source task is created.

For string keys, configure a fixed-width ASCII column using a binary/ASCII-compatible database collation. Locale-aware
and variable-width strings are not safe range keys.
