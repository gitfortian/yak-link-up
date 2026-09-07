# Print Sink Design

为 Link-Up 增加 `print` Sink Connector:把收到的每一行数据以规范格式写入**任务日志**,零外部依赖。与 `datagen` Source(见 DATA_GEN_SOURCE_DESIGN.md)构成零外部系统的完整 source → sink 参考链路,用于本地测试、Sink/引擎联调与演示。

## 1. 定位与价值

Print Sink 是引擎自测的"标准信号接收器":

- **链路终点**:任意 Source Connector 都可以接 print Sink 完成端到端验证,不需要搭目标端环境。
- **确定性断言**:输出为规范 JSON 行,配合 datagen 的 `rows` 预置数据或 `seed`,可以逐字段比对"读到的 == 打印的"。
- **管道观察**:`print_interval_millis` 模拟慢 Sink,验证背压、取消与 Metrics 行为。
- **演示**:一分钟搭出"JDBC → print"抽查链路,肉眼确认抽样数据。

一条判断标准:它只输出日志,不引入任何持久化、事务或重放概念。

## 2. 范围与非目标

### Stage 1(本文范围)

```text
输出:   slf4j INFO 日志(任务日志文件内),schema 行 + JSON 行
选项:   print_rows、print_interval_millis
提交:   无耐久提交,默认 TASK_LOCAL 语义
```

### 非目标(明确不做)

```text
输出到 stdout / 文件 / 网络端点      -> 文件写出归 File Sink,端点归未来 Sink
多种输出格式(json/csv/表格/彩色)   -> JSON 是唯一规范输出,机器可断言优先
CDC RowKind 标注                    -> FluxRow 无变更语义
行去重 / 排序 / 采样                -> 观察工具不做数据加工
多表                                -> Stage 2,与各 Connector 多数据集一并评估
日志限流(采样打印)               -> 高量场景用 print_rows=false 或减小 row_count
```

## 3. 模块与包结构

新模块 `link-up-connectors/link-up-connector-print`,identifier `print`。**零第三方依赖**:JSON 序列化用 Jackson(经由 link-up-api 传递),日志用 slf4j-api。

```text
com.link.up.connector.print/
├── config/   PrintSinkOptions、PrintSinkConfig
└── sink/     PrintSinkFactory、PrintSinkWriter、PrintRowFormatter
```

`PrintRowFormatter` 是纯函数格式器(输入 dataset/split/行号/schema/FluxRow,输出字符串),不依赖 logger——单元测试直接断言字符串,不需要日志捕获设施。

## 4. 角色链(遵循 CODE_STYLE.md §10)

```text
PrintSinkFactory  (SPI 入口:optionRule / capabilities / createPreparer / createSink)
  -> SinkPreparer   (使用 SinkFactory 默认实现,不覆写)
  -> PrintSinkWriter (open -> write -> prepareCommit -> commit/abort -> close)
```

- **Preparer 显式留空**:`SinkFactory.createPreparer` 的默认实现返回"以 Source 表为准"的 PreparedSinkMetadata。print 没有目标端 DDL、没有连接校验,任何"准备"动作都是编造副作用;不覆写就是明确语义。
- `createSink(config, metadata)` 实现 metadata-aware 重载(新推荐风格),metadata 仅透传,不派生状态。
- Writer 每任务一个,不共享;`write(batch, sourceTable)` 中的 `sourceTable` 提供列名与类型,列映射/重命名后的投影结果由框架传入,Writer 原样使用。

## 5. 配置与 Option 规则

### 5.1 Options

| Option | 类型 | 默认 | Scope | 说明 |
| --- | --- | --- | --- | --- |
| `print_rows` | boolean | `true` | TASK | 是否打印行数据;`false` 时只打印 schema 行,用于只验证链路不看数据 |
| `print_interval_millis` | long | `0` | RUNTIME | 每个 batch 写入后的模拟延迟,观察背压/取消时显式开启 |

刻意不提供的选项:输出目标(stdout/file)与输出格式——两者都会把"观察工具"变成"又一个 Sink",职责越界;JobSpec `mapping` 的列裁剪已够用。

### 5.2 OptionRule

```text
required:   无(print 是零配置可用的最简 Sink)
optional:   print_rows、print_interval_millis
```

取值校验在 `PrintSinkConfig.of` 完成(interval ≥ 0)。

## 6. 输出格式

### 6.1 行结构

每个数据集(dataset)首次写入时打一行 schema;此后每行数据一条 INFO 日志:

```text
schema: dataset=datagen_table columns=[id<BIGINT>, name<STRING>, age<INT>]
data:   dataset=datagen_table split=datagen#0 row=1 {"id":1000,"name":"mibkqpoa","age":27}
data:   dataset=datagen_table split=datagen#0 row=2 {"id":1001,"name":"tqzwnhcf","age":41}
```

- 前缀 `dataset / split / row` 由 `PrintRowFormatter` 固定产出:`row` 是 Writer 内从 1 递增的行号(仅本 Writer 计数);并行 Writer 各有计数器,**跨 split 不构成全序**,断言必须用"行集合比较"而不是顺序比较——这是并行引擎的事实,测试工具不掩饰它。
- JSON 字段名取自 `sourceTable` schema 列名(经过 JobSpec mapping 投影后的名字),顺序与 FluxRow 字段序一致。
- 日志走 slf4j INFO:任务日志自然落入 Worker 的 job log 文件(`GET /api/v1/jobs/{jobId}/logs` 可查),MDC/jobId 上下文由框架边界维护;不使用 `System.out`,避免与引擎生命周期日志交错、逃避 job log 归档。

### 6.2 类型映射矩阵(Flux → JSON)

| Flux 类型 | JSON 输出 | 说明 |
| --- | --- | --- |
| STRING | string | |
| BOOLEAN | true/false | |
| tinyint/smallint/int/bigint | number | |
| float/double | number | |
| decimal | string(`toPlainString`) | JSON number 会被二进制浮点重解释,保精度用字符串 |
| bytes | string(base64) | |
| date / time / timestamp | ISO-8601 字符串 | `yyyy-MM-dd` / `HH:mm:ss` / `yyyy-MM-ddTHH:mm:ss` |
| timestamp_tz | ISO-8601 带偏移字符串 | |
| null | JSON null | |

无法识别的字段值类型(不该出现)按 IllegalStateException 传播——格式器是纯函数,任何异常都是 Connector 自身 bug。

## 7. 提交语义与 Retry

```text
write()      -> 仅产生日志行,无外部副作用
prepareCommit() / commit() / abort()  -> no-op(默认实现)
getCommitScope()                      -> 默认 TASK_LOCAL
getRetryAdvice()                      -> 默认文案
```

print 的"提交"不产生任何耐久状态,Retry 永远不存在数据安全问题;不覆写 commit 生命周期——为 no-op Sink 包装一套提交协议,只会污染 Commit Evidence 语义的示范性。它同时是无事务 Sink 的参考实现:其他 Connector 评审时对比"哪些覆写是真实语义,哪些是仪式"。

## 8. Capability 与错误语义

```text
capabilities = 空集
```

不声明任何 Capability:无 schema 发现、无分区、无 UPSERT、无两阶段提交。`discoverTableSchemas` 是 Source 侧契约,Sink 不涉及。

错误语义:配置错误构造期抛出;运行期除 sleep 中断外无预期失败路径。日志内容即用户数据——`print_rows` 打开的 Job 日志包含明文数据,文档向用户提示不要在含敏感数据的任务上开启(与 CODE_STYLE §9 "日志不得输出 secret" 的边界一致:print 的输出对象是用户显式要求观察的数据,不是 Connector options)。

## 9. 测试计划

```text
PrintSinkConfigTest
  shouldDefaultPrintRowsToTrue
  shouldRejectNegativeInterval

PrintRowFormatterTest
  shouldFormatSchemaLineWithColumnNamesAndTypes
  shouldFormatRowAsJsonWithDeclaredColumnNames
  shouldRenderDecimalAsPlainString
  shouldEncodeBytesAsBase64
  shouldRenderDateAndTimestampAsIso8601
  shouldRenderNullAsJsonNull
  shouldIncludeDatasetSplitAndRowNumberInPrefix

PrintSinkWriterTest
  shouldLogSchemaOncePerDataset
  shouldNotLogRowsWhenPrintRowsDisabled
  shouldSleepBetweenBatchesWithoutBusyWait
  shouldStopOnInterruptDuringInterval

PrintSinkFactoryTest
  shouldExposeIdentifierPrint
  shouldExposeEmptyCapabilities
  shouldUseDefaultNoopPreparer
```

引擎级参考链路(datagen → print)冒烟归 DATA_GEN_SOURCE_DESIGN.md §9,此处不重复建设。

## 10. 交付清单

1. 新模块 `link-up-connector-print`(零第三方依赖)+ 注册进 `link-up-connectors/pom.xml`。
2. `link-up-launcher`、`link-up-dist` 依赖清单加入新模块。
3. 实现顺序:`config` → `sink`(formatter 先行)→ SPI 注册。
4. 更新 `CONNECTOR_ADAPTATION.md` 增加 Print Sink 章节;`README.md` 模块表补充。
