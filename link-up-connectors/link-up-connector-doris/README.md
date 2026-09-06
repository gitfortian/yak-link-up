# Link-Up Doris Connector

`link-up-connector-doris` provides Doris Catalog metadata support, Stream Load Sink support, and a bounded native Source.

## Native Source

The Source data path follows the Doris native scanner design used by SeaTunnel:

```text
DorisSource
  -> Doris Catalog metadata discovery (MySQL-compatible protocol)
  -> FE POST /api/{database}/{table}/_query_plan
  -> opaqued query plan + tablet routings
  -> deterministic tablet/BE splits
  -> BE TDorisExternalService openScanner/getNext/closeScanner
  -> Arrow IPC batch
  -> FluxRow
```

The JDBC/MySQL protocol is used only for table metadata discovery. Actual table data is not read through JDBC.

### Supported in this stage

- bounded/offline reads only
- single-table and multi-table reads
- Catalog-backed schema discovery
- `doris.read.field` column projection
- `doris.filter.query` predicate pushdown into the FE query plan
- tablet-level split planning and BE parallelism
- Doris FE failover/retry for `_query_plan`
- Doris v1 query-plan response and v2 `{code,msg,data}` envelope compatibility
- direct BE Thrift scanner reads
- Arrow IPC -> `FluxRow` decoding for scalar fields
- per-table `doris.request.tablet.size`, `doris.batch.size`, and `doris.exec.mem.limit`
- `LARGEINT` exposed as `STRING` for the native Source so the full signed 128-bit range is preserved

### Explicitly out of scope

- CDC / streaming reads
- Arrow Flight SQL mode
- asynchronous Arrow decode queues
- ARRAY / MAP / STRUCT native conversion
- HLL / BITMAP aggregate-type reads
- runtime schema evolution
- JDBC data-read fallback

Arrow Flight SQL is intentionally not the default implementation here. Doris documents it as a high-performance option, but it currently has a different execution boundary and does not provide the same tablet-level multi-BE parallel read model. The FE query-plan + BE scanner path maps directly onto Link-Up's `SourceSplitEnumerator` / `SourceReader` architecture.

## Source configuration

Single table:

```hocon
source {
  Doris {
    fenodes = "fe-1:8030,fe-2:8030"
    username = "root"
    password = ""
    database = "analytics"
    table = "orders"

    doris.read.field = "id,order_no,amount,created_at"
    doris.filter.query = "id >= 100"
    doris.request.tablet.size = 8
    doris.batch.size = 1024
    doris.request.connect.timeout.ms = 30000
    doris.request.read.timeout.ms = 30000
    doris.request.query.timeout.s = 3600
    doris.request.retries = 3
    doris.exec.mem.limit = 2147483648
  }
}
```

Multiple tables:

```hocon
source {
  Doris {
    fenodes = "fe-1:8030,fe-2:8030"
    username = "root"
    password = ""

    table_list = [
      {
        database = "analytics"
        table = "orders"
        doris.read.field = "id,amount"
        doris.filter.query = "status = 'PAID'"
        doris.request.tablet.size = 4
      },
      {
        database = "crm"
        table = "customers"
      }
    ]
  }
}
```

Compatibility aliases such as `scan_filter`, `request_tablet_size`, `scan_batch_rows`, `scan_connect_timeout_ms`, `scan_read_timeout_ms`, `scan_query_timeout_sec`, `max_retries`, and `scan_mem_limit` are accepted for consistency with other Link-Up native sources.

## Type boundary

The native Arrow decoder supports Link-Up scalar targets: boolean, integer widths, floating point, decimal, string, bytes, date, time, and timestamp.

Doris `LARGEINT` is signed 128-bit and is normalized to Link-Up `STRING` in the native Source. This avoids silently narrowing values into an insufficient decimal precision.

Complex and aggregate types are deliberately conservative in this stage. `ARRAY`, `MAP`, `STRUCT`, `HLL`, and `BITMAP` fail during source schema preparation rather than being converted through `String.valueOf(...)` and producing ambiguous data.
