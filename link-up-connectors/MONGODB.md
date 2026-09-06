# MongoDB Connector

MongoDB follows Link-Up's bounded/offline boundary. The product goal is usability first: users select a database, collection and fields; they do not declare BSON or Link-Up field types.

## Stage 1 — Catalog and automatic schema discovery

Stage 1 established the metadata boundary:

- module `link-up-connector-mongodb`
- MongoDB Java synchronous driver 4.11.5
- database -> MongoDB database
- table -> collection
- automatic sampled schema discovery
- BSON -> canonical Link-Up type inference
- numeric widening and heterogeneous-type fallback
- dotted nested field discovery for BSON documents
- conservative JSON/STRING boundary for documents and arrays
- `_id` primary-key metadata when discovered
- synthetic `_id` metadata for an empty collection

Default discovery limits are connector-internal:

```text
schema sample size = 1000 documents
schema max depth   = 8
```

Yak Ops should not ask users to provide field types.

## BSON type policy

```text
ObjectId        -> STRING
String          -> STRING
Boolean         -> BOOLEAN
Int32           -> INT
Int64           -> BIGINT
Double          -> DOUBLE
Decimal128      -> DECIMAL(34,18)
DateTime        -> TIMESTAMP
Timestamp       -> TIMESTAMP
Binary          -> BYTES
Document        -> STRING (Extended JSON boundary)
Array           -> STRING (Extended JSON boundary)
other BSON      -> STRING
Null / missing  -> nullable
```

Physical BSON information remains in `Column.sourceType` and `Column.attributes`; Mongo-specific types do not leak into Link-Up's global `SqlType` model.

Schema discovery observes multiple documents rather than trusting the first one. Numeric values widen when safe and incompatible shapes degrade to STRING. Missing sampled fields become nullable.

## Nested documents

A document field remains a JSON string boundary and its child fields are also discovered as dotted paths.

```json
{
  "name": "Yak",
  "address": {
    "province": "Sichuan",
    "city": "Chengdu"
  }
}
```

Discovery exposes:

```text
name
address
address.province
address.city
```

This keeps the Link-Up row schema flat while allowing a future Yak Ops field picker to render a tree.

## Stage 2 — bounded MongoDB Source

Stage 2 adds the finite read path:

```text
MongoDB collection
  -> sampled TableSchema
  -> one bounded collection split
  -> Mongo cursor
  -> BSON projection/filter
  -> MongoBsonRowConverter
  -> FluxRow
```

The Source exposes only `TABLE_SCHEMA_DISCOVERY`. It deliberately does not advertise `PARTITION_SPLIT` or `MULTI_TABLE` yet.

### Configuration

Minimal configuration:

```hocon
source {
  type = "mongodb"
  uri = "mongodb://127.0.0.1:27017/app"
  collection = "users"
}
```

The database may be selected explicitly when it is not present in the URI:

```hocon
source {
  type = "mongodb"
  uri = "mongodb://127.0.0.1:27017"
  database = "app"
  collection = "users"
}
```

Field selection requires names only, never types:

```hocon
source {
  type = "mongodb"
  uri = "mongodb://127.0.0.1:27017/app"
  collection = "users"

  fields = ["_id", "username", "address.city", "created_at"]
  fetch_size = 1000
}
```

An optional bounded `find` filter accepts MongoDB Extended JSON:

```hocon
filter = """{"status":"ACTIVE"}"""
```

### Projection

Explicit `fields` are pushed down to MongoDB. Dotted paths are supported.

If both a parent and one of its children are selected, for example:

```text
address
address.city
```

only the parent path is sent in the MongoDB projection to avoid a parent/child projection collision. The row converter still derives both selected Link-Up fields from the returned document.

When no fields are selected, Stage 2 reads the full document. This avoids manufacturing a projection from sampled metadata and remains robust when the collection contains fields that were not seen during discovery.

### Runtime conversion and schema drift

The prepared `TableSchema` remains authoritative while a bounded task is running.

- ObjectId is emitted as its hex STRING form.
- DateTime/Timestamp values become UTC `LocalDateTime` values.
- Documents, arrays and other STRING fallback values use Extended JSON.
- Missing fields become null.
- Safe numeric widening follows the discovered Link-Up type.
- A later document that conflicts with a strongly inferred scalar type fails explicitly instead of being silently coerced.

This is deliberate: a sampled schema can never prove that every document in a schema-less collection has the same shape. Silent coercion would make downstream relational writes harder to reason about.

### Single-split boundary

Stage 2 always creates one full-collection split, even when Source reader parallelism is greater than one.

This is intentional. MongoDB `_id` is not guaranteed to be an ObjectId and may be a string, integer, UUID, compound/application value, or another BSON type. Stage 2 does not pretend that a safe generic range partition exists.

A later hardening stage may add explicit range/chunk planning with a conservative single-split fallback.

### Bounded does not mean transactional snapshot

Stage 2 executes one finite MongoDB `find` cursor. It does not claim a point-in-time multi-reader database snapshot or exactly-once snapshot semantics. Concurrent application updates may affect what MongoDB returns according to the server/read-concern configuration supplied by the connection URI.

## Explicit non-goals for Stage 2

- MongoDB Sink
- multi-collection execution
- automatic partition/range split planning
- Change Streams / oplog / CDC
- realtime polling
- checkpoint/savepoint
- automatic runtime schema evolution
- user-authored schema/type configuration
- job-level exactly-once snapshot claims

## Next stage

```text
Stage 1  Catalog + schema discovery              DONE
Stage 2  bounded MongoDB Source                  DONE in this PR
Stage 3  bounded MongoDB Sink + offline hardening
```
