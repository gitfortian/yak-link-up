# MongoDB Connector

MongoDB follows Link-Up's bounded/offline boundary. The product goal is usability first: users select a database, collection and fields; they do not declare BSON or Link-Up field types.

## Status

```text
Stage 1  Catalog + automatic schema discovery     DONE (#117)
Stage 2  bounded MongoDB Source                   DONE (#118)
Stage 3  bounded MongoDB Sink                     DONE in this PR
```

After Stage 3, the connector has a complete single-collection bounded Source/Sink path. CDC, multi-collection execution and generic partition splitting remain separate future work rather than being mixed into the core offline contract.

## Stage 1 — Catalog and automatic schema discovery

MongoDB has no enforced table schema. Link-Up therefore treats:

```text
MongoDB database   -> Link-Up database
MongoDB collection -> Link-Up table
MongoDB field      -> Link-Up column
```

`MongoCatalog#getTable` samples collection documents and synthesizes a stable `TableSchema`.

Default discovery limits are connector-internal:

```text
schema sample size = 1000 documents
schema max depth   = 8
```

Yak Ops should not ask users to provide field types.

### BSON -> Link-Up discovery policy

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

### Nested documents

A document field remains a JSON string boundary and child fields are also discovered as dotted paths.

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

This keeps the Link-Up row schema flat while allowing Yak Ops to render a field tree.

## Stage 2 — bounded MongoDB Source

The finite read path is:

```text
MongoDB collection
  -> sampled TableSchema
  -> one bounded collection split
  -> Mongo cursor
  -> BSON projection/filter
  -> MongoBsonRowConverter
  -> FluxRow
```

The Source exposes only `TABLE_SCHEMA_DISCOVERY`. It does not advertise `PARTITION_SPLIT` or `MULTI_TABLE`.

### Source configuration

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

Field selection requires names only:

```hocon
fields = ["_id", "username", "address.city", "created_at"]
```

An optional bounded `find` filter accepts MongoDB Extended JSON:

```hocon
filter = """{"status":"ACTIVE"}"""
```

Explicit fields are pushed down to MongoDB. If both a parent and child are selected, only the parent is sent in the server projection, while the row converter still derives both Link-Up fields.

A later document that conflicts with a strongly inferred scalar type fails explicitly instead of being silently coerced.

Stage 2 always uses one full-collection split. `_id` is not guaranteed to be ObjectId or generically range-partitionable, so reader parallelism does not manufacture unsafe split semantics.

## Stage 3 — bounded MongoDB Sink

The bounded write path is:

```text
FluxRow
  -> prepared source TableSchema
  -> MongoFluxRowBsonConverter
  -> ordered local document buffer
  -> insertMany(ordered=true)
  -> MongoDB acknowledgement
```

Stage 3 is deliberately INSERT-only. It does not expose `UPSERT`, `AUTO_CREATE_TABLE`, `MULTI_TABLE` or two-phase-commit capabilities.

### Sink configuration

Minimal configuration:

```hocon
sink {
  type = "mongodb"
  uri = "mongodb://127.0.0.1:27017/archive"
  collection = "users_copy"
}
```

When the URI has no database:

```hocon
sink {
  type = "mongodb"
  uri = "mongodb://127.0.0.1:27017"
  database = "archive"
  collection = "users_copy"
}
```

Optional batching and stable document identity:

```hocon
sink {
  type = "mongodb"
  uri = "mongodb://127.0.0.1:27017/archive"
  collection = "orders"

  batch_size = 1000
  document_id_field = "order_id"
}
```

`document_id_field` copies one existing source field to MongoDB `_id`. The original source field is still retained in the document. If the source schema already contains `_id`, configuring another `document_id_field` is rejected as ambiguous.

If the source contains `_id`, Stage 3 preserves it. A null implicit `_id` is omitted so MongoDB may generate one. When Stage 1 metadata proves that a STRING originated from BSON ObjectId, Stage 3 restores the 24-hex value to a real BSON ObjectId for MongoDB-to-MongoDB round trips.

### Target collection creation

MongoDB naturally creates a missing collection on the first successful insert. Stage 3 allows that native behavior, but does not advertise Link-Up `AUTO_CREATE_TABLE`: the connector does not run an explicit collection-DDL contract, configure validators, or evolve target schema.

### Link-Up -> BSON policy

```text
STRING          -> BsonString
Mongo ObjectId STRING metadata -> BsonObjectId
Mongo Extended JSON STRING metadata -> original BSON Document/Array when unambiguous
BOOLEAN         -> BsonBoolean
TINYINT         -> BsonInt32
SMALLINT        -> BsonInt32
INT             -> BsonInt32
BIGINT          -> BsonInt64
FLOAT           -> BsonDouble
DOUBLE          -> BsonDouble
DECIMAL         -> BsonDecimal128
BYTES           -> BsonBinary
DATE            -> ISO-8601 BsonString
TIME            -> ISO-8601 BsonString
TIMESTAMP       -> BsonDateTime when millisecond-exact
TIMESTAMP_TZ    -> ISO-8601 BsonString preserving offset
NULL            -> BsonNull
```

MongoDB BSON DateTime has millisecond precision. A Link-Up TIMESTAMP containing sub-millisecond precision is rejected instead of being silently truncated.

Native Link-Up ARRAY/MAP/ROW values are not accepted in Stage 3. MongoDB-origin documents and arrays already cross the generic pipeline as STRING + Extended JSON metadata, which Stage 3 can restore when the metadata is unambiguous.

### Dotted paths and Mongo round trips

Relational dotted field names are interpreted as MongoDB nested paths:

```text
customer.name -> { "customer": { "name": ... } }
```

Ordinary overlapping paths such as `customer` plus `customer.name` are rejected because the result would otherwise depend on write order.

MongoDB-origin parent documents are a special safe case. When Stage 1 metadata proves that `address` is an Extended-JSON Mongo Document, Stage 3 allows both:

```text
address
address.city
```

The parent document is restored first and the explicit child field deterministically overwrites that nested value. This keeps the default MongoDB Source schema usable for MongoDB-to-MongoDB copy jobs.

### Flush and durability boundary

`batch_size` defaults to 1000 documents.

```text
write()
  -> buffer documents
  -> threshold reached
  -> ordered insertMany

prepareCommit()
  -> flush remaining documents

commit()
  -> task lifecycle boundary only

abort()
  -> discard only unsent local buffer

close()
  -> never implicitly flush
```

Every successful `insertMany` is already a remote write and cannot be rolled back by a later task abort. The writer therefore reports `TASK_LOCAL` commit scope and makes no Job-level atomicity or exactly-once claim.

The target write concern must be acknowledged. Stronger write concerns configured in the MongoDB URI remain valid; an unacknowledged write concern is rejected.

### Failure and retry boundary

Stage 3 does not automatically replay a failed `insertMany` request.

Even with `ordered=true`, MongoDB may have committed a prefix of the batch before returning an error such as a duplicate key. A network failure may also leave the exact remote outcome unclear. After one insert failure, the writer enters a failed state and refuses additional writes or `prepareCommit` replay.

Whole-task retry therefore requires target verification. Stable `_id` values can make duplicate detection easier, but they do not turn this INSERT Sink into an UPSERT or exactly-once connector.

## Explicit non-goals

The completed bounded connector still deliberately excludes:

- Change Streams / oplog / CDC
- realtime polling or streaming semantics
- automatic runtime schema evolution
- update / replace / delete operations
- exposed UPSERT semantics
- multi-collection execution / `MULTI_TABLE`
- generic automatic range/chunk Source splitting / `PARTITION_SPLIT`
- checkpoint/savepoint recovery
- job-level exactly-once or point-in-time snapshot claims
- MongoDB transactions spanning Link-Up SinkTasks

## Future optional hardening

Future work should only be added when the semantics are explicit:

```text
safe Source range/chunk planning with single-split fallback
multi-collection source/sink routing with explicit target mapping
optional collection validators / explicit create semantics
```

These are intentionally not required for the core usability goal: select a MongoDB database/collection/fields and run a bounded offline synchronization job without manually declaring types.
