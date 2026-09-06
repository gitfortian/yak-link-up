# Elasticsearch Connector Family

Stage 0 只建立模块与依赖边界，不实现 Source / Sink，也不注册 SPI factory。

## Module layout

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

对外预留两个独立 connector identifier：

```text
elasticsearch7
elasticsearch8
```

`elasticsearch-common` 不是用户可选择的 Connector，只承载两个版本都能复用的 Link-Up / Elasticsearch 产品语义。

## Dependency boundary

`elasticsearch-common` 不允许 import 或依赖任何 Elasticsearch vendor SDK。

版本依赖只能存在于叶子模块：

```text
common  -X-> ES SDK
es7     -X-> es8
es8     -X-> es7
```

当前版本 pin：

- ES7 client: `org.elasticsearch.client:elasticsearch-rest-high-level-client:7.17.29`
- ES8 client: `co.elastic.clients:elasticsearch-java:8.19.21`

ES8 8.19 仍支持 Java 8。Stage 0 排除 `elasticsearch-java` 的 Jackson 3 runtime artifacts，避免把 Java 17-only mapper implementation 带入 Link-Up 的 Java 8 基线；具体 JSON mapper 在 ES8 implementation stage 再决定。

## Runtime boundary

Link-Up 当前 `launcher` 会直接依赖 built-in connectors，`dist` 再把 runtime dependencies 平铺到同一个 `lib/` classpath。

ES7 High Level REST Client 和 ES8 Java API Client 都会使用 `org.elasticsearch.client:elasticsearch-rest-client`，但版本不同。因此仅拆 Maven module 不能保证两个版本同时运行时安全。

Stage 0 明确禁止把 `elasticsearch7` / `elasticsearch8` 直接加入 `link-up-launcher` 或 `link-up-server`。测试会守住这个边界，直到后续引入并验证 connector classloader isolation、shade/relocation 或其他等价的 dependency island 方案。

## Stage boundary

Stage 0 包含：

- `common / es7 / es8` 三个 Maven module
- 独立 client version pin
- 稳定且不同的 connector identifiers
- compile/test classpath 隔离测试
- flattened runtime guard

Stage 0 不包含：

- Source / Sink factory
- Schema / mapping / query / bulk 实现
- Catalog
- runtime plugin loader 改造
- CDC、实时同步或 RowKind 语义
