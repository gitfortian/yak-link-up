# DataGen Source Design

为 Link-Up 增加 `datagen` Source Connector:一个**零外部依赖**的模拟数据源,按用户声明的 schema 在内存中生成有界数据。用于本地测试、Sink Connector 隔离验证、引擎端到端演示,以及 CI 中作为通用测试上游。与 `print` Sink(见 PRINT_SINK_DESIGN.md)构成零外部系统的完整 source → sink 参考链路。

## 1. 定位与价值

DataGen Source 是引擎自测的"标准信号发生器":

- **validate/explain 全路径可用**:零 IO,不触任何外部系统,是验证 validate/explain 契约的理想载体。
- **Sink 隔离测试**:任意 Sink Connector 都可以只配 datagen Source 完成端到端验证,不需要搭上游环境。
- **CI 通用上游**:架构 guard、Capability 协商、Retry 语义等引擎测试用它造数据,不 mock 框架内部。
- **演示**:一分钟搭出"datagen → print / datagen → JDBC"演示链路。

一条判断标准:它只生成数据,不引入任何执行机制;不为"模拟失败/慢流"增加引擎新概念(见 §7 的克制边界)。

## 2. 范围与非目标

### Stage 1(本文范围)

```text
类型:    Flux 基础标量类型(见 §6 类型矩阵)
生成:    range / values / sequence 三种列级模式,seed 确定性复现
rows:    row_count 随机生成 或 rows 显式预置数据(二选一)
split:   行区间均分,split_count 个 bounded splits
并行:    与引擎现有 Split 分配/Metrics/生命周期完全复用
```

### 非目标(明确不做)

```text
MAP / ARRAY / ROW 复合类型生成      -> 有真实测试需求再加
向量类型(float_vector 等)         -> SqlType 无此类型
CDC 语义(INSERT/UPDATE/DELETE 事件)-> FluxRow 无变更语义,违反 bounded
自定义生成函数 / 脚本表达式         -> 安全面与抽象成本不成比例
多表                              -> Stage 2,与文件 Source 多数据集一并评估
Schema 自动随机生成               -> schema 必填,测试要的是显式契约
模拟节点故障 / 异常注入            -> 引擎测试用已有结构化错误机制,不造新概念
```

## 3. 模块与包结构

新模块 `link-up-connectors/link-up-connector-datagen`,identifier `datagen`。**零第三方依赖**——生成只用 JDK `java.util.Random`,是"能用 JDK 解决的简单问题,不额外引库"的满分场景。

```text
com.link.up.connector.datagen/
├── config/   DataGenSourceOptions、DataGenSourceConfig、ColumnRule
└── source/   DataGenSourceFactory、DataGenSource、DataGenSourceSplit、
              DataGenSourceSplitEnumerator、DataGenSourceReader、DataGenRowGenerator
```

不新增 `generator` 角色包:生成器是 Source 的核心实现细节,不与外部类型系统打交道,留在 `source` 包内。`config` 的校验原则与 file/print 同构——`DataGenSourceConfig.of(ReadonlyConfig)` 构造期完成全部校验,构造完成即合法。

## 4. 角色链与 Split 模型

执行链与所有 Source 同构:`Factory -> Source -> SplitEnumerator -> Reader`。

```java
DataGenSourceSplit implements SourceSplit
├── splitId      // "datagen#<seq>"
├── dataSetId    // table_name
├── startRow     // 行区间起点(含)
└── rowCount     // 本 split 行数
```

- **行区间均分**:`row_count` 按上下取整均分给 `split_count` 个 split,余数行并入最后一个 split。区间确定、顺序确定,同一配置 + 同一 seed 完全可复现。
- `split_count` 默认取 `SourceEnumeratorContext.getParallelism()`,与并行度天然对齐;显式配置时覆盖。
- rows 显式模式下,预置行同样按区间均分。
- Reader 与 MongoSourceReader 同构:`open(splits)` → 逐 split `readBatch()` → `closeSplit()` → `close()`;生成行不持有 IO 资源,`close` 只做状态复位。

**全局总行数语义**:`row_count` 是整个 Job 的总行数,而不是每个 Reader 各生成一份。测试要断言"写入目标端的行数 == 配置值",按 Reader 计数会让断言乘上并行度,是测试工具的陷阱。

## 5. 配置与 Option 规则

### 5.1 Options

| Option | 类型 | 默认 | Scope | 说明 |
| --- | --- | --- | --- | --- |
| `schema` | list<object> | 必填 | TASK | `{name, type, nullable, min, max, length, values, sequence_start}`;生成相关字段可选,见 §6 |
| `row_count` | long | `5` | TASK | 全局总行数(非每 Reader 行数) |
| `rows` | list<list<string>> | 无 | TASK | 显式预置数据,按 schema 字段顺序;设置后忽略 `row_count`;bytes 用 base64 编码 |
| `split_count` | int | = parallelism | RUNTIME | bounded split 数,最小 1,不超过总行数 |
| `read_interval_millis` | long | `0` | RUNTIME | 相邻 batch 间模拟延迟,测 Sink 流控时显式开启 |
| `seed` | long | 无 | TASK | 随机种子;设置后同配置生成完全相同的数据 |
| `table_name` | string | `datagen_table` | TASK | dataSetId / 目标逻辑表名 |

### 5.2 OptionRule

```text
required:   schema
optional:   其余全部
exclusive:  rows 与 row_count 互斥
```

`split_count` 与 `read_interval_millis` 的取值校验在 `DataGenSourceConfig.of` 完成(split_count ≥ 1、interval ≥ 0、rows 与 schema 列数一致)。

## 6. 生成器语义

### 6.1 列级规则

生成规则内嵌到 schema 列定义,每列独立——同类型的列往往表达不同业务字段(age 与 score 的取值域毫无关系),按类型做全局配置会迫使所有同类型列共享一个取值域,再把差异抹平。

```hocon
schema = [
  { name = "id",    type = "bigint", sequence_start = 1000 }
  { name = "name",  type = "string", length = 12 }
  { name = "city",  type = "string", values = ["hangzhou", "shanghai"] }
  { name = "age",   type = "int",    min = 18, max = 60 }
  { name = "score", type = "double" }
]
```

模式推导(无需显式 mode 字段,规则简单且冲突即报错):

```text
有 values          -> pick      从候选列表随机抽取(含边界)
有 sequence_start  -> sequence  从起始值递增,跨 split 连续(split 边界按全局行号推算)
有 min / max       -> range     [min, max] 闭区间随机
其余               -> 类型默认 range(见类型矩阵)
```

### 6.2 类型矩阵

| Flux 类型 | 默认 range | 支持模式 | 转换规则 |
| --- | --- | --- | --- |
| string | 长度 8 随机小写字母 | values / range(length) | `length` 控制长度 |
| boolean | true/false 均匀 | values | |
| tinyint/smallint/int/bigint | 0 ~ 类型最大值 | range / values / sequence | |
| float/double | 0.0 ~ 1000.0 | range / values | |
| decimal | 0 ~ 10000,scale 按声明 | range(precision/scale 内) | 超出 precision/scale fail-fast |
| date | 2024-01-01 + 随机 3650 天 | values(`yyyy-MM-dd`) | |
| time | 00:00:00 ~ 23:59:59 | values(`HH:mm:ss`) | |
| timestamp | 2024-01-01 + 随机 3650 天 | values | 不带时区,对齐 TIMESTAMP 语义 |

不支持声明类型外的字段;`bytes` Stage 1 不支持生成(仅 rows 预置数据中以 base64 提供,与 HOCON 文本协议的限制一致)。

### 6.3 确定性

`seed` 是测试工具的本质需求:随机数据无法复现失败,无法断言。实现约束:

- 全部随机数来自以 `seed` 初始化的单个 `java.util.Random`,生成为纯函数:`f(config, 全局行号) -> FluxRow`。
- Reader 按全局行号(而非消费顺序)取值,split 分配顺序、batch 大小变化都不影响最终数据集。
- 未设 seed 时行为不变,只是不可复现(演示场景)。

## 7. Reader 语义

```text
readBatch()  -> 生成行填满 batchSize 或当前 split 耗尽;
                read_interval_millis > 0 时 batch 间 sleep
close()      -> 状态复位,无外部资源
```

- `Thread.sleep` 必须正确处理中断:捕获 `InterruptedException` 后恢复中断标志并终止读取(遵循 CODE_STYLE.md §8),取消语义由框架 CancellationToken 在外层驱动。
- `rows` 模式:行值以文本形态给出,按 schema 类型转换后输出,转换失败在 openSplit 阶段 fail-fast(测试数据错误应该最早暴露)。
- 生成异常(不该发生)按未预期 IllegalStateException 传播——生成器是纯函数,任何异常都是 Connector 自身 bug。

## 8. Capability 与错误语义

```text
capabilities = 空集
```

不声明任何 Capability:schema 是显式契约而非"发现"(`TABLE_SCHEMA_DISCOVERY` 不成立);split 是合成行区间而非数据分区(`PARTITION_SPLIT` 不成立)。datagen 不应让 Capability 协商产生关于外部系统的假信号。

`discoverTableSchemas()` 直接从配置构建 `CatalogTable`(零 IO),validate 与 explain 语义下都合法。`TablePath` 取 `table_name` 规范化形式。

错误语义:配置错误构造期抛出(不存在外部资源类错误);运行期除 sleep 中断外无预期失败路径。

## 9. 测试计划

```text
DataGenSourceConfigTest
  shouldRejectRowsWhenColumnCountMismatch
  shouldRejectSplitCountBeyondTotalRows
  shouldRejectUnsupportedGeneratorType
  shouldPreferRowsOverRowCount

DataGenRowGeneratorTest
  shouldProduceIdenticalDataWithSameSeed          (核心:确定性)
  shouldProduceIndependentDataWithDifferentSeed
  shouldDistributeSequenceContinuouslyAcrossSplits
  shouldPickValuesInclusiveOfBoundaries
  shouldConvertPresetRowsByDeclaredType

DataGenSourceSplitEnumeratorTest
  shouldSplitRowRangeWithRemainderInLastSplit
  shouldDefaultSplitCountToParallelism

DataGenSourceReaderTest
  shouldRespectReadIntervalWithoutBusyWait
  shouldStopOnInterruptDuringInterval
  shouldReturnEndOfInputAfterAllSplits
```

另需一个引擎级冒烟:datagen(rows 显式数据)→ print sink,断言日志行与预置数据逐字段一致——这条同时锁住 DataGenSource 与框架 Split 分配的契约,是 datagen → print 参考链路的基线用例。

## 10. 交付清单

1. 新模块 `link-up-connector-datagen`(零第三方依赖)+ 注册进 `link-up-connectors/pom.xml`。
2. `link-up-launcher`、`link-up-dist` 依赖清单加入新模块。
3. 实现顺序:`config` → `source`(generator 先行)→ SPI 注册。
4. 更新 `CONNECTOR_ADAPTATION.md` 增加 DataGen Source 章节;`README.md` 模块表补充。
5. 可选:框架测试用 datagen Source 替换手工造数,建立引擎级冒烟基线(不阻塞本 Stage)。
