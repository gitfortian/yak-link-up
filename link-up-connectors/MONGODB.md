# MongoDB Connector

MongoDB connector follows Link-Up's bounded/offline boundary. The product goal is usability first: users select a database, collection and fields; they do not declare BSON or Link-Up field types.

## Stage 1 — Catalog and automatic schema discovery

Stage 1 provides metadata discovery only. It deliberately does not register a SourceFactory or SinkFactory and does not enter launcher/server runtime composition yet.

Implemented:

- module `link-up-connector-mongodb`
- MongoDB Java synchronous driver 4.11.5
- `MongoCatalog`
- database -> MongoDB database
- table -> collection
- automatic sampled schema discovery
- BSON -> canonical Link-Up type inference
- numeric widening and heterogeneous-type fallback
- dotted nested field discovery for BSON documents
- conservative JSON/STRING boundary for documents and arrays
- `_id` primary-key metadata when discovered
- synthetic `_id` metadata for an empty collection so metadata discovery still returns a usable schema

Default discovery limits:

```text
schema sample size = 1000 documents
schema max depth   = 8
```

These are connector-internal defaults. Yak Ops should not ask users to provide field types.

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

## Merge policy

Schema discovery observes multiple documents instead of trusting the first document.

```text
INT + BIGINT            -> BIGINT
INT/BIGINT + DOUBLE     -> DOUBLE
INT/BIGINT + DECIMAL    -> DECIMAL
DECIMAL + DOUBLE        -> STRING
NULL + T                -> nullable T
incompatible scalar     -> STRING
complex + incompatible  -> STRING
```

When a field is missing from part of the sample, it becomes nullable. Multiple BSON physical types are retained in the `mongodb.bsonTypes` column attribute.

## Nested documents

A document field is retained as a JSON string boundary and its child fields are also discovered as dotted paths.

Example:

```json
{
  "name": "Yak",
  "address": {
    "province": "Sichuan",
    "city": "Chengdu"
  }
}
```

Discovered columns include:

```text
name
address
address.province
address.city
```

This lets a future Yak Ops field picker expose a tree while keeping the Link-Up row schema flat and portable to JDBC/Doris/StarRocks sinks.

Arrays remain JSON strings in Stage 1. Strong ARRAY/ROW inference is intentionally deferred until cross-sink compatibility has a clear product requirement.

## Explicit non-goals for Stage 1

- bounded Source reader
- Sink writer
- partition/split planning
- multi-table execution
- CDC / Change Streams / oplog
- realtime semantics
- checkpoint/savepoint
- automatic schema evolution
- user-authored schema/type configuration

## Next stages

```text
Stage 1  Catalog + schema discovery     DONE in this PR
Stage 2  bounded MongoDB Source
Stage 3  bounded MongoDB Sink + offline hardening
```
