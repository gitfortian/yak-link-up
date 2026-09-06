# Elasticsearch Connector Family

Elasticsearch 按产品族维护，对外保留两个明确的 connector identifier：

```text
elasticsearch7
elasticsearch8
```

代码模块保持三层：

```text
link-up-connector-elasticsearch-common
  -> link-up-api

link-up-connector-elasticsearch7
  -> elasticsearch-common
  -> ES 7.17 High Level REST Client

link-up-connector-elasticsearch8
  -> elasticsearch-common
  -> ES 8.19 Java API Client
```

`elasticsearch-common` 不是用户可选择的 Connector，只承载版本无关的 Link-Up / Elasticsearch 产品语义，并且不允许依赖 Elasticsearch vendor SDK。

## Dependency boundary

```text
common  -X-> ES SDK
es7     -X-> es8
es8     -X-> es7
```

当前版本 pin：

- ES7 client: `org.elasticsearch.client:elasticsearch-rest-high-level-client:7.17.29`
- ES8 client: `co.elastic.clients:elasticsearch-java:8.19.21`
- ES8 Jackson 2 mapper runtime: `com.fasterxml.jackson.core:jackson-databind:2.18.8`

Link-Up 保持 Java 8。ES8 Java API Client 8.19.21 本身以 Java `--release 8` 编译；ES8 leaf 显式采用 `JacksonJsonpMapper` + Jackson 2.18.8，并继续排除 Java 17-only 的 Jackson 3 runtime artifacts。其他模块现有 Jackson 版本不变。

## Stage 0 — module and runtime boundary

已完成 `common / es7 / es8` module、独立 client version pin、稳定 connector identifier 和 flattened runtime guard。

当前 `launcher` / `dist` 仍使用扁平 runtime classpath。ES7 和 ES8 带入不兼容版本的 `org.elasticsearch.client:elasticsearch-rest-client`，因此在 connector classloader isolation、shade/relocation 或等价方案完成前，两套 Elasticsearch connector 仍禁止直接进入 launcher/server runtime。

## Stage 1 — Elasticsearch 7 bounded Source

已实现：

- `TableSourceFactory` identifier `elasticsearch7`
- 单 index / 单 index alias mapping discovery
- bounded Scroll + sliced-scroll splits
- `_source` projection、Query DSL、Basic Auth
- ES major version=7 校验和 clear-scroll 生命周期
- mapping -> `TableSchema` -> `FluxRow`

## Stage 2 — Elasticsearch 7 bounded Sink

已实现：

- `SinkFactory` identifier `elasticsearch7`
- 一个已存在的 target index / single-index alias
- target mapping 与 source schema 预校验
- 同步 Bulk `IndexRequest`
- `batch_size` + `prepareCommit()` flush
- 可选稳定 `document_id_field`
- 仅对明确返回的 408/429/502/503/504 item failure 做重试
- whole-request IOException 不自动重试
- task-local durability；成功 Bulk 不可回滚
- 不声明 UPSERT / CDC / realtime capability

## Stage 3 — Elasticsearch 8 bounded Source

已实现：

- `TableSourceFactory` identifier `elasticsearch8`
- 单 index / 单 index alias mapping discovery
- `RestClientTransport + ElasticsearchClient`
- `JacksonJsonpMapper`（Jackson 2.18.8）
- bounded Scroll + sliced-scroll splits
- `_source` projection、ES8 Query DSL JSON、Basic Auth
- ES major version=8 校验
- clear-scroll 生命周期
- mapping -> `TableSchema` -> `FluxRow`

Stage 3 继续使用 bounded sliced Scroll。PIT + `search_after` 涉及跨 split 的共享 PIT 生命周期、一致性、清理与恢复，不在当前离线 connector 范围内顺带引入。

## Stage 4 — Elasticsearch 8 bounded Sink

已实现：

- `SinkFactory` identifier `elasticsearch8`
- 一个已存在的 target index，或解析到单一 concrete index 的 alias
- SinkPreparer 在 writer 启动前校验 ES major=8、target mapping、source/target compatibility
- typed Java API Client `BulkRequest` + `IndexOperation<Map<String,Object>>`
- `batch_size` 达阈值 flush；`prepareCommit()` flush 剩余文档；`close()` 不隐式 flush
- 可选 `document_id_field` 转换为稳定 `_id`
- `_id` 非空且限制在 512 UTF-8 bytes
- 仅重试 Bulk response 中明确失败且状态为 408/429/502/503/504 的 item
- 只重试失败 item，不重发已明确成功 item
- whole-request IOException / request-level exception 不自动 retry，因为服务端成功范围不可判定
- task-local durability；`abort()` 只能丢弃未发送数据，已成功 Bulk 无法回滚
- dotted field 重建为 JSON object，并拒绝 `customer` + `customer.name` 这类父子路径冲突
- 对 ES8 新增 structured mappings（如 `rank_vectors` / `aggregate_metric_double`）维持保守 STRING(JSON)/ROW/ARRAY 边界
- 不声明 UPSERT、CDC、RowKind 或实时写入能力

示例：

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

目标 index 必须预先存在，Stage 4 不自动创建 index，也不修改 mapping。

## Shared bounded semantics

ES7 / ES8 两套 Source + Sink 现在保持同一产品边界：

- bounded / offline only
- one index per task
- no CDC / streaming / RowKind
- no automatic schema evolution
- no job-level transaction / exactly-once claim
- stable `_id` 只是重跑确定性手段，不等同于向 Link-Up 暴露 UPSERT capability

类型策略保持保守：确定的 boolean/integer/floating scalar 才做强类型；复杂 mapping 使用 STRING/JSON 等可控边界。Elasticsearch mapping 无法表达 scalar-vs-array cardinality，因此 Source 不猜 Array；Sink 不做不安全 numeric narrowing。

## Compatibility boundary

`elasticsearch7` client 固定为 `7.17.29`，当前首要兼容目标为 Elasticsearch 7.17.x。

`elasticsearch8` client 固定为 `8.19.21`，当前首要兼容目标为 Elasticsearch 8.19.x。

更广 minor version 兼容范围应在真实集成测试后再扩大声明。

## Implementation status

```text
Stage 0  module / dependency boundary        DONE
Stage 1  Elasticsearch 7 bounded Source      DONE
Stage 2  Elasticsearch 7 bounded Sink        DONE
Stage 3  Elasticsearch 8 bounded Source      DONE
Stage 4  Elasticsearch 8 bounded Sink        DONE
```

后续若要让 ES7 与 ES8 同时进入正式 launcher/server runtime，需要单独完成 connector runtime dependency isolation；这不属于 bounded Source/Sink 适配本身。
