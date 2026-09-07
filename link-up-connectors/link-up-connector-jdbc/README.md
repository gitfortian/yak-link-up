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

The shared `gbase/common` layer only carries stable product metadata and family-level helpers. Runtime dialects remain
product-specific: URL parsing, identifiers, catalog behavior, type mapping, row conversion, Sink SQL and MPP behavior
must stay in the concrete product adapter unless completed implementations prove that behavior is genuinely common.

The planned bounded/offline implementation order is:

1. GBase 8c Source, then existing-table Sink.
2. GBase 8a Source, then existing-table JDBC Sink; native/high-speed MPP loading is a later stage.
3. GBase 8s Source, then existing-table Sink.

CDC, compatibility-mode expansion, automatic distributed-table design and product-native bulk-loading paths stay outside
this family-level contract.

### GBase 8a bounded Source + existing-table JDBC Sink

GBase 8a is exposed as the first-class `gbase8a` JDBC dialect and uses the vendor JDBC protocol/driver:

```hocon
source {
  type = "jdbc"
  url = "jdbc:gbase://gbase8a:5258/app"
  driver = "com.gbase.jdbc.Driver"
  dialect = "gbase8a"
  table_path = "app.orders"
}

sink {
  type = "jdbc"
  url = "jdbc:gbase://gbase8a:5258/archive"
  driver = "com.gbase.jdbc.Driver"
  dialect = "gbase8a"
  table = "orders"
  schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
  data_save_mode = "APPEND_DATA"
}
```

GBase 8a uses `database.table` semantics with no separate schema layer in this bounded JDBC stage. The JDBC URL owns the
active database. Source metadata may come from another database/schema, but an unqualified Sink target is always rebound
to the Sink URL database so source routing metadata cannot leak into the target. An explicit target may be `orders` or
repeat the URL database as `archive.orders`; an explicit different database fails during Sink preparation instead of
silently redirecting one connection to another database.

Metadata discovery remains based on standard JDBC `DatabaseMetaData` for databases, tables, columns and primary keys.
The Catalog now implements `WritableCatalog` only to participate in the shared JDBC Sink save-mode lifecycle. The stage
supports metadata validation and `TRUNCATE TABLE` for `DROP_DATA`, while structure-changing DDL remains blocked:

- no CREATE/DROP DATABASE
- no CREATE/DROP TABLE
- no ADD COLUMN or runtime schema evolution
- no automatic target-table creation

Create the target table before running the job. `CREATE_SCHEMA_WHEN_NOT_EXIST` is safe for an existing compatible table
but fails clearly when the table is absent. `RECREATE_SCHEMA` cannot destructively drop a table because DROP TABLE is
blocked before recreation. `CREATE_OR_ADD_COLUMNS` may validate an already compatible target, but a missing column fails
instead of mutating MPP table structure.

The Sink reuses the shared JDBC writer: parameterized `INSERT`, configured batch size, `PreparedStatement.addBatch()` /
`executeBatch()`, one task-local transaction and the existing commit/rollback, retry/savepoint and dirty-data behavior.
`rewriteBatchedStatements=true` is a GBase 8a dialect default because the vendor JDBC driver can rewrite batched INSERTs
into multi-value INSERT statements; explicit user `properties` may override it.

UPSERT/MERGE is intentionally not advertised. GBase 8a MERGE has distribution-specific constraints, so this generic
existing-table stage does not guess HASH distribution keys or business-key semantics. POC/native high-speed loading is
also a later stage; this PR keeps JDBC batch INSERT as the usable baseline before introducing a separate MPP-native load
path.

The read-side type contract covers common numeric, string/text, binary/BLOB, DATE, TIME, DATETIME and TIMESTAMP types.
DATETIME/TIMESTAMP map to the Link-Up `TIMESTAMP` boundary. DECIMAL values above Link-Up's precision limit fall back to
STRING instead of silently losing precision. Target database type generation remains disabled because this stage never
auto-creates GBase 8a tables.

For large bounded reads, a positive Link-Up `fetch_size` selects the GBase JDBC streaming-result sentinel
`Integer.MIN_VALUE`. `tinyInt1isBit=false` and `yearIsDateType=false` remain safe type defaults. The Source advertises
`BEST_EFFORT` read consistency only and does not claim one MPP-wide snapshot across parallel JDBC readers.

The vendor GBase JDBC driver is not guessed as a Maven dependency by this module. Deployments must provide the official
GBase 8a JDBC driver on the runtime classpath and configure `driver = "com.gbase.jdbc.Driver"`.

### GBase 8s bounded Source

GBase 8s is exposed as the first-class `gbase8s` JDBC dialect. Stage 1 uses the native GBase 8s JDBC protocol and keeps
normal GBase SQL semantics separate from later SQLMODE compatibility work:

```hocon
source {
  type = "jdbc"
  url = "jdbc:gbasedbt-sqli://gbase8s:9088/app:GBASEDBTSERVER=gbase01;IFX_LOCK_MODE_WAIT=10"
  driver = "com.gbasedbt.jdbc.Driver"
  dialect = "gbase8s"
  schema = "gbasedbt" # optional table owner; defaults to username
  table_path = "gbasedbt.orders"
}
```

The database and `GBASEDBTSERVER` instance identity are connection-level concerns and must be present in the JDBC URL or
connection properties. One Stage 1 Source connection is bound to the database in its URL. GBase 8s reports table owner
through the JDBC schema field, so Link-Up models physical tables as `database.owner.table` metadata while SQL inside the
selected database uses `owner.table`. The normal user-facing paths are `table` and `owner.table`; a three-part
`database.owner.table` path is accepted only when its database equals the URL database. Cross-database rebinding is not
performed implicitly.

The read-only `GBase8sCatalog` uses standard JDBC `DatabaseMetaData` for owner, table, column and primary-key discovery.
If no owner is supplied, the connector uses the explicit `schema` option and then the JDBC username as the default owner.
When neither gives an owner and the same table name is visible under multiple owners, metadata preparation fails and asks
for an explicit `owner.table` instead of guessing.

GBase 8s JDBC defaults `DELIMIDENT=n`. In that mode double-quoted SQL identifiers are not valid, so the dialect does not
blindly quote every table/column name. Ordinary unquoted identifiers are normalized to GBase 8s lowercase behavior. If a
deployment explicitly enables `DELIMIDENT=y` in the URL or JDBC properties, quoted `table_path` parts preserve case and
SQL identifiers are emitted with double quotes. This keeps the default path compatible while still allowing deliberate
case-sensitive database objects.

The Stage 1 type boundary covers common built-ins without exposing GBase JDBC private objects:

- BOOLEAN -> BOOLEAN
- SMALLINT -> SMALLINT
- SERIAL / INTEGER / INT -> INT
- INT8 / SERIAL8 / BIGINT / BIGSERIAL -> BIGINT
- SMALLFLOAT / REAL -> FLOAT
- FLOAT / DOUBLE PRECISION -> DOUBLE
- DEC / DECIMAL / NUMERIC / MONEY -> DECIMAL when precision fits Link-Up
- DATE -> DATE
- DATETIME -> TIMESTAMP
- BYTE / BLOB -> BYTES
- CHAR / VARCHAR / LVARCHAR / NCHAR / NVARCHAR / TEXT / CLOB -> STRING
- INTERVAL -> STRING
- unknown, opaque and extension JDBC types -> STRING

DECIMAL precision above Link-Up's limit falls back to STRING rather than silently truncating numeric precision. INTERVAL
also stays STRING in this stage because the GBase 8s JDBC driver represents intervals with vendor-specific
`com.gbasedbt.lang.Interval*` classes. Target type generation remains disabled until the dedicated 8s Sink stage.

The Source supports bounded single-table and multi-table jobs, custom SQL and the shared safe JDBC range partition
planner. It advertises `BEST_EFFORT` read consistency only. Although GBase 8s provides transactional isolation levels,
this stage does not claim a coordinated point-in-time snapshot across independent parallel JDBC readers.

Out of scope for this stage: JDBC Sink, WritableCatalog, automatic DDL, SQLMODE MySQL/Oracle compatibility expansion,
CDC/realtime synchronization, logical-log integration, coordinated snapshots, complex ROW/COLLECTION native objects and
runtime schema evolution.

The vendor JDBC driver is not guessed as a Maven dependency by this module. Deployments must provide the official GBase
8s JDBC driver on the runtime classpath and configure `driver = "com.gbasedbt.jdbc.Driver"` (or an older vendor-provided
compatible driver class when required by that deployment).

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
