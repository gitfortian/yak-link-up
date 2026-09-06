# Link-Up Elasticsearch 8 Connector

The `elasticsearch8` connector provides bounded Source and Sink support through Elasticsearch Java API Client 8.19.21.

## Bounded Source

Supported:

- one concrete index, or an alias resolving to one concrete index
- mapping-based schema discovery
- bounded Scroll reads
- sliced-scroll parallelism
- Query DSL JSON
- `_source` projection
- Basic Auth
- explicit clear-scroll lifecycle
- server major-version validation (`8`)

```hocon
source {
  Elasticsearch8 {
    hosts = ["http://127.0.0.1:9200"]
    index = "orders"
    username = "elastic"
    password = "secret"
    source = ["order_id", "amount", "customer.name"]
    query = """{"range":{"created_at":{"gte":"2026-01-01"}}}"""
    scroll_time = "1m"
    scroll_size = 1000
    slices = 4
  }
}
```

## Bounded Sink

Supported:

- exactly one source table and one existing target index
- alias only when it resolves to one concrete target index
- target mapping discovery and conservative source/target compatibility validation
- synchronous typed Java API Client Bulk index operations
- `batch_size` flushing and `prepareCommit()` final flush
- optional `document_id_field` for stable `_id`
- retry only explicit item failures with 408/429/502/503/504
- capped exponential retry backoff
- task-local durability boundary
- `close()` never implicitly flushes

```hocon
sink {
  Elasticsearch8 {
    hosts = ["http://127.0.0.1:9200"]
    index = "orders_target"
    username = "elastic"
    password = "secret"
    document_id_field = "order_id"
    batch_size = 1000
    max_retries = 3
    retry_backoff_ms = 200
    max_retry_backoff_ms = 5000
  }
}
```

The target index must already exist. Stage 4 does not auto-create indices or evolve mappings.

`document_id_field` improves deterministic re-indexing but does not expose CDC/UPSERT semantics and does not provide job-level atomicity. Successful Bulk requests are already durable and cannot be rolled back by `abort()`.

Whole-request Bulk failures are not automatically retried because the server-side success range is ambiguous. Only individual failed items explicitly returned by Elasticsearch with retryable status codes are retried.

## Java / JSON boundary

Link-Up remains on Java 8. Elasticsearch Java API Client 8.19.21 is compiled with Java `--release 8`. The ES8 leaf explicitly provides Jackson 2.18.8 and uses `JacksonJsonpMapper`; Jackson 3 runtime artifacts remain excluded because they require Java 17.

Source and Sink share one package-local ES8 client-resource initializer for hosts, Basic Auth, timeouts, Jackson mapper, `RestClientTransport`, and `ElasticsearchClient`. No Elasticsearch SDK type is moved into `elasticsearch-common`.

## Type boundary

The connector intentionally keeps a conservative mapping boundary:

- boolean/integer/floating scalar mappings use Link-Up scalar types
- integer writes only allow safe widening
- `unsigned_long` uses `DECIMAL(20,0)` on reads and validates the full unsigned 64-bit range on writes
- date values stay at STRING/DATE/TIMESTAMP boundaries
- object/nested/geo/vector/range and newer structured ES8 mappings use STRING(JSON)/ROW/ARRAY where safe
- Elasticsearch mappings do not declare scalar-vs-array cardinality, so typed Source fields that actually contain arrays fail explicitly rather than silently guessing an Array type
- runtime schema evolution is not supported

## Deliberately excluded

- automatic index creation or mapping evolution
- `UpdateRequest` / `DeleteRequest`
- exposed UPSERT semantics
- PIT / `search_after`
- Elasticsearch SQL
- wildcard or comma-separated multi-index reads/writes
- aliases resolving to multiple concrete indices
- CDC / streaming / RowKind semantics
- job-level transaction / exactly-once claims

The module is still not wired into the current flat launcher/server runtime classpath. ES7 and ES8 remain separate dependency islands until runtime connector isolation is completed.
