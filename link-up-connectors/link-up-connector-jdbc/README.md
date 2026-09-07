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

GBase 8c, GBase 8a and GBase 8s now support bounded Source, JDBC Sink and safe automatic creation of a missing target
table. Each product keeps its own target-type and DDL semantics. GBase 8c resolves the actual database compatibility mode
before automatic DDL; users do not configure A/B/C/PG manually. GBase 8a native/high-speed MPP loading remains a later
performance stage.

CDC, automatic distributed-table design, unverified compatibility-mode expansion and product-native bulk-loading paths
stay outside this family-level contract.

### GBase 8c bounded Source + JDBC Sink + compatibility-aware automatic target creation

GBase 8c is exposed as the first-class `gbase8c` JDBC dialect and uses the dedicated GBase 8c JDBC protocol:

```hocon
source {
  type = "jdbc"
  url = "jdbc:gbase8c://gbase8c:5432/app"
  driver = "com.gbase8c.Driver"
  dialect = "gbase8c"
  schema = "public"
  table_path = "public.orders"
}

sink {
  type = "jdbc"
  url = "jdbc:gbase8c://gbase8c:5432/archive"
  driver = "com.gbase8c.Driver"
  dialect = "gbase8c"
  schema = "landing"
  table = "orders"
  schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  data_save_mode = "APPEND_DATA"
}
```

One GBase 8c JDBC connection stays bound to the database in its URL. SQL uses `schema.table` inside that database. For an
implicit Sink target, Link-Up keeps only the source table name and resolves database/schema from the Sink connection, so
source routing metadata cannot leak into the target even when source and target database names happen to match. Explicit
`schema.table` targets are preserved; an explicit three-part target must repeat the Sink URL database.

When the target table is missing, Link-Up resolves the target database's actual `pg_database.datcompatibility` value and
recognizes the stable GBase 8c modes `A`, `B`, `C` and `PG`. The mode is detected from the database itself instead of being
exposed as another user-facing connector option. Unknown/newer modes fail closed for automatic DDL rather than silently
assuming PostgreSQL semantics. Ordinary existing-table jobs do not query compatibility mode unless a documented
mode-specific metadata difference needs normalization.

Automatic DDL copies only the relational shape needed by the bounded Sink:

- column names
- compatibility-aware target types
- NULL / NOT NULL
- source primary key only when the shared `create_primary_key` option keeps it

It deliberately does **not** copy source DEFAULT/identity/SERIAL/AUTO_INCREMENT behavior, comments, indexes, foreign
keys, partitions, storage orientation, replication or hash-distribution policy. The generated baseline contains no
`DISTRIBUTE BY` or replication clause; Link-Up does not guess a physical MPP distribution key from source metadata.

The compatibility-aware target type baseline keeps stable GBase/PostgreSQL-core scalar types where they round-trip safely:

- TINYINT / SMALLINT -> SMALLINT
- INT -> INTEGER
- BIGINT -> BIGINT
- FLOAT -> REAL
- DOUBLE -> DOUBLE PRECISION
- DECIMAL -> NUMERIC(precision, scale)
- BOOLEAN -> BOOLEAN
- BYTES -> BYTEA
- TIME -> TIME
- TIMESTAMP -> TIMESTAMP
- B/C/PG DATE -> DATE
- A-mode DATE -> TIMESTAMP(0) WITHOUT TIME ZONE
- PG-mode STRING with a safe known length -> VARCHAR(length)
- A/B/C STRING -> TEXT

A/B/C string targets use `TEXT` because GBase 8c documents PG-mode CHAR/VARCHAR length in characters while the other
stable compatibility modes count bytes; a cross-database source length is therefore not safely reusable as a target byte
limit. `TIMESTAMP_TZ` is enabled only for the currently verified PG-mode contract. ARRAY/MAP/ROW and other types without a
safe automatic target contract fail before DDL execution.

A-mode DATE has a deliberate physical/logical round-trip rule. GBase 8c represents A-mode DATE as
`TIMESTAMP(0) WITHOUT TIME ZONE`, so JDBC metadata reads that physical column back as TIMESTAMP. GBase 8c Sink validation
normalizes only the corresponding source-DATE/target-TIMESTAMP pair back to logical DATE for A mode; the generic JDBC
conversion contract remains unchanged for every other database and mode. This keeps a table created on the first job
compatible when the same job runs again.

`CREATE_SCHEMA_WHEN_NOT_EXIST` creates a missing table and validates an existing table. `CREATE_OR_ADD_COLUMNS` may also
create a missing table, but it still cannot mutate an existing target: `ADD COLUMN` remains blocked. `RECREATE_SCHEMA`
cannot destructively recreate an existing table because `DROP TABLE` remains blocked. CREATE/DROP DATABASE is also
disabled. The target database/schema must already exist and the JDBC user must have permission to create a table there.

After preparation, row writing still reuses the shared JDBC path: parameterized `INSERT`, configured batch size,
`PreparedStatement.addBatch()` / `executeBatch()`, task-local transaction, commit/rollback, savepoint retry and dirty-data
handling. UPSERT/MERGE, runtime schema evolution, CDC and native bulk-loading remain out of scope.

The vendor GBase 8c JDBC driver is not guessed as a Maven dependency by this module. Deployments must provide the official
driver on the runtime classpath and configure `driver = "com.gbase8c.Driver"`.

### GBase 8a bounded Source + JDBC Sink + safe automatic target creation

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
  schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  data_save_mode = "APPEND_DATA"
}
```

GBase 8a uses `database.table` semantics with no separate schema layer in this bounded JDBC stage. The Sink JDBC URL owns
the target database. Source metadata may come from another database/schema, but an unqualified target is always rebound
to the Sink URL database so source routing metadata cannot leak into the target. An explicit target may be `orders` or
repeat the URL database as `archive.orders`; an explicit different database fails during Sink preparation.

When `schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST` and the target table is absent, Link-Up now creates a conservative
target table automatically before writing rows. `CREATE_OR_ADD_COLUMNS` also creates a missing table, but it still does
not mutate an existing table: `ADD COLUMN` remains blocked. `RECREATE_SCHEMA` remains non-destructive because `DROP TABLE`
is still blocked. CREATE/DROP DATABASE is also disabled.

Automatic DDL copies only the relational shape needed to receive synchronized rows:

- column names
- portable GBase 8a target types
- NULL / NOT NULL
- source primary key only when the shared `create_primary_key` option keeps it

It deliberately does **not** copy source defaults, AUTO_INCREMENT/identity behavior, comments, indexes, foreign keys,
partitions, compression, replication or distribution-key policy. The generated SQL contains no `DISTRIBUTED BY` or
`REPLICATED` clause. Link-Up therefore does not guess a business hash key; the target GBase 8a version/database owns its
default physical distribution behavior.

The automatic target type contract is conservative:

- BOOLEAN -> BOOLEAN
- TINYINT / SMALLINT / INT / BIGINT -> matching integer type
- FLOAT / DOUBLE -> matching floating type
- DECIMAL -> DECIMAL(precision, scale), with GBase limits validated
- STRING with a known safe length up to 8191 -> VARCHAR(length)
- longer or unknown-length STRING -> LONGTEXT
- BYTES -> LONGBLOB
- DATE -> DATE
- TIME -> TIME
- TIMESTAMP -> DATETIME

`TIMESTAMP_TZ`, ARRAY, MAP, ROW and other types without a safe portable GBase 8a target contract fail before unsafe DDL is
executed rather than being silently narrowed. Source auto-increment metadata is never copied because the JDBC Sink writes
the source value explicitly.

The Sink continues to reuse the shared JDBC writer: parameterized `INSERT`, configured batch size,
`PreparedStatement.addBatch()` / `executeBatch()`, one task-local transaction and the existing commit/rollback,
retry/savepoint and dirty-data behavior. `rewriteBatchedStatements=true` remains a GBase 8a dialect default; explicit user
`properties` may override it.

UPSERT/MERGE is intentionally not advertised. GBase 8a MERGE has distribution-specific constraints, so this generic stage
does not guess HASH distribution keys or business-key semantics. POC/native high-speed loading is also a later stage.

The read-side type contract still covers common numeric, string/text, binary/BLOB, DATE, TIME, DATETIME and TIMESTAMP
types. DATETIME/TIMESTAMP map to the Link-Up `TIMESTAMP` boundary. DECIMAL values above Link-Up's read precision limit
fall back to STRING instead of silently losing precision.

For large bounded reads, a positive Link-Up `fetch_size` selects the GBase JDBC streaming-result sentinel
`Integer.MIN_VALUE`. `tinyInt1isBit=false` and `yearIsDateType=false` remain safe type defaults. The Source advertises
`BEST_EFFORT` read consistency only and does not claim one MPP-wide snapshot across parallel JDBC readers.

The vendor GBase JDBC driver is not guessed as a Maven dependency by this module. Deployments must provide the official
GBase 8a JDBC driver on the runtime classpath and configure `driver = "com.gbase.jdbc.Driver"`.

### GBase 8s bounded Source + JDBC Sink + safe automatic target creation

GBase 8s is exposed as the first-class `gbase8s` JDBC dialect and uses the native GBase 8s JDBC protocol:

```hocon
source {
  type = "jdbc"
  url = "jdbc:gbasedbt-sqli://gbase8s:9088/app:GBASEDBTSERVER=gbase01;IFX_LOCK_MODE_WAIT=10"
  driver = "com.gbasedbt.jdbc.Driver"
  dialect = "gbase8s"
  schema = "gbasedbt" # optional table owner; defaults to username
  table_path = "gbasedbt.orders"
}

sink {
  type = "jdbc"
  url = "jdbc:gbasedbt-sqli://gbase8s:9088/archive:GBASEDBTSERVER=gbase01;IFX_LOCK_MODE_WAIT=10"
  driver = "com.gbasedbt.jdbc.Driver"
  dialect = "gbase8s"
  schema = "gbasedbt" # target owner; defaults to username
  table = "orders"
  schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  data_save_mode = "APPEND_DATA"
}
```

The database and `GBASEDBTSERVER` instance identity are connection-level concerns. One connection stays bound to the
database in its JDBC URL. GBase 8s reports table owner through the JDBC schema field, so Link-Up models physical tables as
`database.owner.table` metadata while SQL inside the selected database uses `owner.table`.

For Sink, the JDBC URL owns the target database. An implicit target keeps only the source table name and resolves owner
from the Sink `schema` option, then the Sink username. Source database/schema/owner metadata is never reused implicitly.
An explicit `owner.table` may choose the target owner, while an explicit three-part target must repeat the database from
the Sink URL. A different database fails during preparation instead of silently rebinding one connection.

When `schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST` and the target table is missing, Link-Up creates the table before
writing rows. The target database and owner themselves must already exist and the JDBC user must have CREATE permission.
`CREATE_OR_ADD_COLUMNS` also creates a missing table, but an existing table with missing target columns still fails because
`ADD COLUMN` remains blocked. `RECREATE_SCHEMA` cannot destructively recreate an existing target because `DROP TABLE`
remains blocked. CREATE/DROP DATABASE is also disabled.

Automatic DDL copies only the relational shape needed by the bounded Sink:

- column names
- portable native GBase 8s target types
- NULL / NOT NULL
- source primary key only when the shared `create_primary_key` option keeps it

It deliberately does **not** copy source DEFAULT expressions, SERIAL/BIGSERIAL generation, comments, indexes, foreign
keys, partitions or SQLMODE-specific DDL. The JDBC writer continues to insert the source value explicitly, so a source
SERIAL/BIGSERIAL column is recreated as an ordinary integer column rather than a new target-side sequence generator.

The automatic target type contract is conservative and targets native/normal GBase 8s mode:

- STRING with a known length up to 8000 -> VARCHAR(length)
- longer or unknown-length STRING -> TEXT
- BOOLEAN -> BOOLEAN
- TINYINT / SMALLINT -> SMALLINT
- INT -> INTEGER
- BIGINT -> BIGINT
- FLOAT -> SMALLFLOAT
- DOUBLE -> FLOAT
- DECIMAL -> DECIMAL(precision, scale), with target precision limited to 32
- BYTES -> BYTE
- DATE -> DATE
- TIME -> DATETIME HOUR TO SECOND
- TIMESTAMP -> DATETIME YEAR TO FRACTION(5)

`TIMESTAMP_TZ`, ARRAY, MAP, ROW and other types without a safe native GBase 8s target contract fail before DDL execution.
`TIMESTAMP WITH TIME ZONE` is not silently introduced because that behavior belongs to compatibility-mode work rather
than the native connector stage.

GBase 8s also restricts large-object columns in unique/reference constraints. If an automatically copied primary key would
map to `TEXT` or `BYTE`, Link-Up rejects the DDL before execution and asks for a bounded scalar target type or
`create_primary_key=false` instead of returning a vendor-specific constraint error later.

GBase 8s JDBC defaults `DELIMIDENT=n`. In that mode automatic target owner/table names and ordinary identifiers are
normalized to lowercase, and identifiers that require quoting are rejected. If `DELIMIDENT=y` is explicitly configured,
quoted/case-preserving owner, table and column identifiers are supported. Target routing, metadata lookup, CREATE TABLE,
SELECT/INSERT and TRUNCATE use the same identifier rule so mixed-case Source metadata cannot create a different physical
target than the one validated during preparation.

The Sink continues to reuse the shared JDBC writer: parameterized `INSERT`, configured batch size,
`PreparedStatement.addBatch()` / `executeBatch()`, one task-local transaction and the existing commit/rollback,
retry/savepoint and dirty-data behavior. The vendor `IFX_USEPUT=1` bulk-insert extension remains **opt-in**, not a Link-Up
default; deployments that have verified compatible scalar target types may enable it explicitly.

UPSERT is intentionally not advertised. Native GBase 8s `MERGE` has a broader database-specific contract than the generic
Link-Up UPSERT abstraction, so this stage does not guess conflict keys or collapse MERGE semantics into
`write_mode=UPSERT`.

The read-side type boundary remains unchanged: SERIAL/INT map to INT, INT8/SERIAL8/BIGINT/BIGSERIAL to BIGINT,
SMALLFLOAT to FLOAT, FLOAT to DOUBLE, DATE/DATETIME to DATE/TIMESTAMP, BYTE/BLOB to BYTES, text types to STRING, and
INTERVAL/unknown extension types stay behind the conservative STRING boundary. The Source advertises `BEST_EFFORT` read
consistency only and does not claim a coordinated point-in-time snapshot across independent JDBC readers.

Still out of scope: CREATE/DROP database, destructive table recreation, runtime ADD COLUMN/schema evolution, SQLMODE
MySQL/Oracle compatibility expansion, CDC/realtime synchronization, logical-log integration, coordinated snapshots and
complex ROW/COLLECTION native object modeling.

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