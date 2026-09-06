# Link-Up Elasticsearch 8 Connector

Stage 3 provides a bounded Elasticsearch 8 Source under connector identifier `elasticsearch8`.

## Bounded Source

Supported:

- one concrete index, or an alias resolving to one concrete index
- mapping-based schema discovery
- bounded Scroll reads through the Elasticsearch Java API Client 8.19.21
- sliced-scroll parallelism
- Query DSL JSON
- `_source` projection
- Basic Auth
- explicit clear-scroll lifecycle
- server major-version validation (`8`)

Example:

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

`slices` defaults to Link-Up Source reader parallelism when omitted.

## Java / JSON boundary

Link-Up remains on Java 8. Elasticsearch Java API Client 8.19.21 is itself compiled with Java `--release 8`. Its Jackson 2 integration is `compileOnly` and built against Jackson 2.18.8, so the ES8 leaf explicitly provides Jackson 2.18.8 and uses `JacksonJsonpMapper`.

Jackson 3 runtime artifacts remain excluded because they require Java 17. The project's existing global Jackson 2.15.4 pin is left unchanged for other modules; the ES8 leaf has its own Jackson 2 pin as part of its dependency island.

No Elasticsearch SDK type is moved into `elasticsearch-common`.

## Type boundary

The Stage 3 mapping boundary intentionally mirrors the ES7 bounded Source:

- boolean/integer/floating scalar mappings use Link-Up scalar types
- `unsigned_long` maps to `DECIMAL(20,0)`
- date/object/nested/geo/vector/range and other complex mappings are preserved as STRING/JSON
- Elasticsearch mappings do not declare scalar-vs-array cardinality, so typed numeric/boolean fields that arrive as arrays fail explicitly rather than silently guessing an array type

## Deliberately excluded

- Elasticsearch 8 Sink (Stage 4)
- PIT / `search_after`
- Elasticsearch SQL
- wildcard or comma-separated multi-index reads
- aliases resolving to multiple concrete indices
- runtime schema evolution
- CDC / streaming / RowKind semantics

The module is still not wired into the current flat launcher/server runtime classpath. ES7 and ES8 remain separate dependency islands until runtime connector isolation is completed.
