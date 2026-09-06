# Link-Up Elasticsearch 7 Connector

Stage 1 provides a bounded Elasticsearch 7 Source under connector identifier `elasticsearch7`.

## Source scope

Supported in this stage:

- one concrete index, or an alias resolving to one concrete index
- mapping-based schema discovery
- bounded Scroll reads
- sliced-scroll parallelism
- Query DSL wrapper query
- `_source` projection
- Basic Auth
- explicit clear-scroll lifecycle

Not supported in this stage:

- Sink
- PIT / search_after
- SQL
- wildcard or multi-index expressions
- aliases resolving to multiple concrete indices
- CDC / streaming / RowKind semantics

The module is intentionally not wired into the current flat launcher/server runtime classpath yet. ES7 and ES8 remain separate dependency islands until runtime connector isolation is completed.

See `../ELASTICSEARCH.md` for the connector-family boundary and configuration example.
