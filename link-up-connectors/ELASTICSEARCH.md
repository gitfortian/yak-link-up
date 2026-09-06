# Elasticsearch Connector Family

Elasticsearch 按产品族维护，但运行时对外保留两个明确的 connector identifier：

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

`elasticsearch-common` 不是用户可选择的 Connector，只承载两个版本都能复用的 Link-Up / Elasticsearch 产品语义，并且不允许 import 或依赖任何 Elasticsearch vendor SDK。

## Dependency boundary

版本依赖只能存在于叶子模块：

```text
common  -X-> ES SDK
es7     -X-> es8
es8     -X-> es7
```

当前版本 pin：

- ES7 client: `org.elasticsearch.client:elasticsearch-rest-high-level-client:7.17.29`
- ES8 client: `co.elastic.clients:elasticsearch-java:8.19.21`
- ES8 Jackson 2 mapper runtime: `com.fasterxml.jackson.core:jackson-databind:2.18.8`

Link-Up 当前仍以 Java 8 为基线。ES8 Java API Client 8.19.21 本身使用 Java `--release 8`；Stage 3 显式采用 `JacksonJsonpMapper` 的 Jackson 2 路径，并继续排除 Java 17-only 的 Jackson 3 runtime artifacts。ES8 的 Jackson 2 pin 仅存在于 ES8 dependency island，不修改其他模块现有 Jackson 版本。

## Stage 0 — module and runtime boundary

Stage 0 已完成 `common / es7 / es8` module、独立 client version pin、connector identifier 和 flattened runtime guard。

Link-Up 当前 `launcher` / `dist` 仍是扁平 runtime classpath。ES7 和 ES8 都依赖不同版本的 `org.elasticsearch.client:elasticsearch-rest-client`，因此在 connector classloader isolation、shade/relocation 或等价 dependency island 方案完成前，两个版本仍禁止直接进入 launcher/server runtime。

## Stage 1 — Elasticsearch 7 bounded Source

Stage 1 已实现：

- `TableSourceFactory` SPI identifier `elasticsearch7`
- 单 index / 单 index alias mapping discovery
- bounded Scroll + sliced-scroll splits
- `_source` projection、Query DSL、Basic Auth
- ES major version=7 校验和 clear-scroll 生命周期
- mapping -> `TableSchema` -> `FluxRow`

Elasticsearch mapping 不区分单值/多值，所以 Stage 1 不自动推断 Array。确定的 boolean/integer/floating 类型保持强类型，其余 date/object/nested/geo/vector/range 等保留为字符串/JSON。

## Stage 2 — Elasticsearch 7 bounded Sink

Stage 2 已实现离线 Bulk 写入：

- `SinkFactory` SPI identifier `elasticsearch7`
- 只支持一个已存在的 target index，或解析到单一 concrete index 的 alias
- SinkPreparer 在 task 启动前校验 ES major version、target mapping、source/target field compatibility
- target index 不存在时直接失败，不做自动建 index
- 同步 Bulk writer，按 `batch_size` flush
- `prepareCommit()` flush 剩余文档；`close()` 不隐式 flush
- 可选 `document_id_field` 作为稳定 `_id`
- Bulk item 明确返回 `408/429/502/503/504` 时按指数 backoff 重试失败项
- 整个 Bulk HTTP 调用抛 IOException 时不自动重试，因为服务端成功范围不可判定
- 成功 Bulk 立即形成 task-local durable boundary，`abort()` 只能丢弃尚未发送的数据

示例：

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

### Stage 2 semantic boundary

Stage 2 使用 Elasticsearch `IndexRequest` 写文档，但不向 Link-Up 暴露 `UPSERT` capability，也不把相同 `_id` 的覆盖行为包装成 CDC UPDATE 语义。

`document_id_field` 未配置时由 Elasticsearch 自动生成 `_id`。此时整任务重试可能产生重复文档；配置稳定 `_id` 可改善重跑确定性，但仍不提供 Job 级事务原子性。

目标 mapping 兼容性保持保守：

- integer 只允许安全 widening
- float -> double 可接受，numeric narrowing 拒绝
- date 接受 STRING/DATE/TIMESTAMP/TIMESTAMP_TZ
- object/nested/geo/vector/range 等复杂字段接受 Link-Up STRING(JSON) / ROW / ARRAY 边界
- `unsigned_long` 在运行时检查非负、整数和 `0..18446744073709551615` 范围
- runtime schema evolution 不支持

明确不包含：

- 自动创建 index / 自动更新 mapping
- UPDATE / DELETE request
- CDC、实时同步、RowKind 语义
- Job-level transaction / exactly-once 声明

## Stage 3 — Elasticsearch 8 bounded Source

Stage 3 已实现独立的 `elasticsearch8` bounded Source，产品语义与 ES7 Source 保持一致，但 vendor API 完全使用 Elasticsearch Java API Client 8.19.21：

- `TableSourceFactory` SPI identifier `elasticsearch8`
- 单 index / 单 index alias mapping discovery
- `RestClientTransport + ElasticsearchClient`
- `JacksonJsonpMapper`（Jackson 2.18.8）
- bounded Scroll + sliced-scroll splits
- `_source` projection、Query DSL JSON、Basic Auth
- ES major version=8 校验
- split 完成 / Reader 关闭时显式 clear scroll
- mapping -> `TableSchema` -> `FluxRow`

示例：

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

### Stage 3 semantic boundary

Stage 3 延续 ES7 Source 的保守类型策略：确定的 boolean/integer/floating scalar mapping 使用强类型，`unsigned_long` 使用 `DECIMAL(20,0)`；date/object/nested/geo/vector/range 等复杂类型保留为 STRING/JSON。mapping 无法声明 scalar-vs-array cardinality，因此强类型字段实际出现数组时直接失败，不静默猜测 Array。

本阶段继续使用 bounded sliced Scroll。虽然 ES8 对 deep pagination 更推荐 PIT + `search_after`，但 PIT 会引入跨 split 的共享 PIT 生命周期、一致性和恢复设计，因此不在 Stage 3 顺带扩入。

明确不包含：

- Elasticsearch 8 Sink
- PIT / `search_after`
- Elasticsearch SQL
- wildcard / comma-separated multi-index read
- alias resolving to multiple concrete indices
- runtime schema evolution
- CDC、实时同步、RowKind 语义

## Compatibility boundary

`elasticsearch7` client 固定为 `7.17.29`，当前首要兼容目标为 Elasticsearch 7.17.x。更早 ES7 minor 需要单独验证后再扩大支持声明。

`elasticsearch8` client 固定为 `8.19.21`，Stage 3 当前首要兼容目标为 Elasticsearch 8.19.x；更广的 ES8 minor 范围在单独验证后再扩大声明。

## Next stage

```text
Stage 4  Elasticsearch 8 bounded Sink
```

所有阶段继续保持离线 / bounded connector 语义，不把 CDC 或实时同步语义顺带引入 Elasticsearch connector family。
