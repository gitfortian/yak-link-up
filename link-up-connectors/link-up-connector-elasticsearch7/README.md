# Link-Up Elasticsearch 7 Connector

The `elasticsearch7` connector now provides bounded Source and Sink support while keeping ES7 isolated from the ES8 SDK dependency island.

## Bounded Source

Supported:

- one concrete index, or an alias resolving to one concrete index
- mapping-based schema discovery
- bounded Scroll reads and sliced-scroll parallelism
- Query DSL wrapper query and `_source` projection
- Basic Auth and explicit clear-scroll lifecycle

## Bounded Sink

Stage 2 supports:

- one existing target index, or an alias resolving to one concrete index
- target mapping validation before writers start
- synchronous Bulk indexing
- configurable `batch_size`
- optional `document_id_field` for stable Elasticsearch `_id`
- retries only for explicit Bulk item status `408/429/502/503/504`
- no automatic retry for transport-level Bulk failures because server-side success is ambiguous
- task-local commit boundary: successful Bulk requests cannot be rolled back

Example:

```hocon
sink {
  Elasticsearch7 {
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

The target index must already exist. Stage 2 intentionally does not auto-create indices or evolve mappings.

`document_id_field` is optional. Without it Elasticsearch generates IDs, so retrying a whole failed task can create duplicate documents. With it the same source key produces a stable `_id`, but the connector still does not claim job-level atomicity or CDC/UPSERT semantics.

## Still out of scope

- PIT / search_after and Elasticsearch SQL
- wildcard or multi-index operations
- aliases resolving to multiple concrete indices
- automatic index creation / mapping evolution
- UPDATE / DELETE / CDC / streaming / RowKind semantics

The module is intentionally not wired into the current flat launcher/server runtime classpath yet. ES7 and ES8 remain separate dependency islands until runtime connector isolation is completed.

See `../ELASTICSEARCH.md` for the connector-family boundary.
