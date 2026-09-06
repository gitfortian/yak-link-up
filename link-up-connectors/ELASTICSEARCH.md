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

Link-Up 当前仍以 Java 8 为基线。ES8 module 继续排除 Jackson 3 runtime artifacts，具体 ES8 JSON mapper 留到 ES8 implementation stage 决定。

## Stage 0 — module and runtime boundary

Stage 0 已完成：

- `common / es7 / es8` 三个 Maven module
- 独立 client version pin
- 稳定且不同的 connector identifiers
- compile/test classpath 隔离测试
- flattened runtime guard

Link-Up 当前 `launcher` 会直接依赖 built-in connectors，`dist` 再把 runtime dependencies 平铺到同一个 `lib/` classpath。ES7 和 ES8 都会使用 `org.elasticsearch.client:elasticsearch-rest-client`，但版本不同，因此仅拆 Maven module 不能保证两个版本同时运行时安全。

在 connector classloader isolation、shade/relocation 或其他等价 dependency island 方案完成并验证前，`elasticsearch7` / `elasticsearch8` 仍禁止直接加入 `link-up-launcher` 或 `link-up-server`。

## Stage 1 — Elasticsearch 7 bounded Source

Stage 1 已实现独立的 `elasticsearch7` bounded Source，保持离线读取语义：

- `TableSourceFactory` SPI identifier: `elasticsearch7`
- 单 index / 单 index alias schema discovery
- Elasticsearch mapping -> Link-Up `TableSchema`
- bounded Scroll 读取
- sliced-scroll split 并行
- framework reader batch 输出
- `_source` 字段投影
- Query DSL wrapper query
- Basic Auth
- server major version=7 校验
- split 结束、异常关闭时清理 scroll context

示例：

```hocon
source {
  Elasticsearch7 {
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

`slices` 未配置时，split 数量跟随 Link-Up Source reader parallelism；显式配置后则保持固定 split 数量。

### Stage 1 type boundary

Elasticsearch mapping 不区分某字段在文档中是单值还是多值，因此 Stage 1 不自动推断 Array 类型。

强类型映射保持在确定范围：

- `boolean` -> `BOOLEAN`
- `byte` -> `TINYINT`
- `short` -> `SMALLINT`
- `integer` / `token_count` -> `INT`
- `long` -> `BIGINT`
- `unsigned_long` -> `DECIMAL(20,0)`
- `half_float` / `float` -> `FLOAT`
- `double` / `scaled_float` / `rank_feature` -> `DOUBLE`

其余类型，包括 `keyword/text/date/date_nanos/ip/binary/object/nested/flattened/geo/vector/range`，Stage 1 统一保留为字符串；对象、嵌套结构和字符串数组会 JSON 序列化。强类型数值/布尔字段如果实际文档出现数组值会直接失败，不做静默猜测。

### Stage 1 scope boundary

本阶段明确不包含：

- Elasticsearch Sink
- PIT / search_after
- Elasticsearch SQL
- wildcard / comma-separated multi-index read
- alias resolving to multiple concrete indices
- runtime schema evolution
- CDC、实时同步、RowKind / DELETE / UPDATE 语义

### ES7 compatibility boundary

`elasticsearch7` 当前 client 固定为 `7.17.29`，因此 Stage 1 首要目标是 Elasticsearch 7.17.x。Elastic High Level REST Client 的兼容保证是同 major 内“client minor <= server minor”，不是 7.17 client 对所有更早 7.x server 的反向保证。更早的 ES7 minor 需要单独验证后再扩大支持声明。

## Next stages

```text
Stage 2  Elasticsearch 7 bounded Sink
Stage 3  Elasticsearch 8 bounded Source
Stage 4  Elasticsearch 8 bounded Sink
```

所有阶段继续保持离线 / bounded connector 语义，不把 CDC 或实时同步语义顺带引入 Elasticsearch connector family。
