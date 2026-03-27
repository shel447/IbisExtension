# 时间处理设计与实现检视

## 1. 检视范围与结论

本次检视重点围绕两类时间入口：

- 场景一：底层字段是 `13` 位 UTC 毫秒 `long/int64`，上层 Ibis 表达式先 `cast("timestamp")`，再按时间类型继续写逻辑
- 场景二：底层字段本身就是原生无时区 `timestamp/datetime`

我重点审阅了以下材料：

- `src/ibis_dsql/compiler.py`
- `src/ibis_dsql/api.py`
- `tests/test_time_handling.py`
- `tests/test_custom_time_handling_scene.py`
- `docs/designs/dsql-syntax-extension-design.md`
- `docs/testing/time-handling-test-matrix.md`

并实际运行了时间相关测试：

```powershell
E:\code\codex_projects\IbisExtension\.venv\Scripts\python.exe -m unittest tests.test_time_handling tests.test_custom_time_handling_scene -v
```

结果：`44` 个用例全部通过。

总体判断：

- 在你当前声明的边界内，这套时间处理设计是成立的，核心方向也是对的。
- 最有价值的设计点是把“比较场景”和“真正的时间语义场景”分开处理，这非常适合大模型生成 Ibis 表达式的约束式用法。
- 当前没有看到“必须立即推翻”的问题，但有几处值得尽快加强，否则后续扩展时容易进入“行为正确但越来越难维护、难验证”的状态。

## 2. 当前设计的优点

### 2.1 场景拆分是清晰的

设计文档把两类入口拆得很清楚：`epoch-ms -> timestamp` 规范链和原生无时区 `timestamp` 列虽然在表达式层都表现为 `timestamp`，但在编译层不等价。这一点在文档中表述得比较完整，代码也确实按这个思路落地了，见：

- `docs/designs/dsql-syntax-extension-design.md:241-288`
- `src/ibis_dsql/compiler.py:494-513`
- `src/ibis_dsql/compiler.py:455-464`

### 2.2 “比较降级为 long，非比较保留时间语义”这个策略是对的

这是当前实现最核心、也最合理的地方。

- 比较场景回到 long 毫秒，避免把原始 long 列包进时间函数，利于保持底层比较语义和潜在索引利用
- `date()/truncate()/strftime()/extract/+/- interval` 这类真正需要“解释时间”的场景，仍然恢复成 timestamp 表达式，避免把时间问题错误地降级成数值问题

这一点同时覆盖了你的两类场景，并且对“大模型生成表达式”也很友好，因为模型只需要遵循统一规范，复杂性收敛在编译器侧。

### 2.3 对真实业务写法的支持比表面看起来更完整

当前实现不只支持 `col.cast("timestamp")` 这种一次性表达式，也支持：

- `mutate(ts=raw.cast("timestamp"))`
- `mutate(ts=ts.cast("timestamp"))`
- 经过 `Field -> relation.values[name]` 的同名字段追溯

这点非常关键，因为真实业务里大模型更可能产出“先 mutate，再 filter/group/order”的链式表达式，而不是一次性内联表达式。相关实现和测试分别在：

- `src/ibis_dsql/compiler.py:42-65`
- `tests/test_time_handling.py:65-114`
- `tests/test_custom_time_handling_scene.py:164-215`

## 3. 主要改进点

### 3.1 最高优先级：不要再把“epoch-ms 语义”寄存在普通 `int -> timestamp` cast 上

这是我认为当前设计里最值得优先调整的点。

当前编译器把所有“整数 cast 到 timestamp”的表达式都视为 `epoch-ms` 规范链入口：

- `src/ibis_dsql/compiler.py:34-39`
- `src/ibis_dsql/compiler.py:536-543`

这意味着目前的业务约束其实是“约定俗成”，而不是“显式建模”：

- 代码并不知道这个字段是不是“13 位 UTC 毫秒”
- 代码也不知道这是不是“你希望走时间规范链”的那个字段
- 它只看到“整数 -> timestamp cast”，就自动套用 `epoch-ms` 语义

这在你当前受控场景下是能工作的，但后续会有两个问题：

1. 语义过载。普通 cast 本来只是类型转换，现在被赋予了业务协议含义。
2. 可扩展性差。以后如果出现“秒级 epoch”、“别的时间编码”、“只是临时 cast 一下”的场景，编译器无法区分。

更稳的做法是把它升格成显式协议，例如：

- 增加一个公开 helper，例如 `epoch_millis_to_timestamp(col)` 或 `normalize_epoch_millis_timestamp(col)`
- 或者给 schema / 列元数据打标记，再让编译器识别这个标记
- 至少也要在 API 层封装一层，而不是直接复用普通 `cast("timestamp")`

这样做的价值不是“功能更多”，而是把业务约束从“暗约定”升级成“显式契约”。

### 3.2 高优先级：把“本地时间视角”从口头约定升级为可验证契约

你已经明确说明“暂时不特别考虑时区，统一站在本地时间视角”，而文档和代码目前也确实是这么实现的：

- 文档同时写了“底层是 13 位 UTC 毫秒”和“时间字符串按本地时间解释”：`docs/designs/dsql-syntax-extension-design.md:344-356`、`402-412`
- 编译器核心依赖 `FROM_UNIXTIME(...)` 和 `UNIX_TIMESTAMP(...)`：`src/ibis_dsql/compiler.py:362-444`
- 目前只在“epoch-ms 与 timezone-aware timestamp 混合比较”时直接报错：`src/ibis_dsql/compiler.py:490-503`

这说明当前设计不是“没有时区语义”，而是“默认把数据库会话时区当成业务本地时间语义”。

这个前提并不一定错，但它必须被显式化，否则跨环境时会出现不易察觉的漂移：

- 开发库、测试库、生产库的 session timezone 不一致
- 同一份 SQL 在不同环境下 `UNIX_TIMESTAMP` / `FROM_UNIXTIME` 结果不同
- 原始数据名义上是 UTC 毫秒，但被本地时间口径解释

建议至少做三件事：

1. 在对外文档中把契约写死：当前实现依赖“数据库 session timezone == 业务本地时间口径”。
2. 增加一条启动检查或集成测试，验证目标库的 session timezone 满足约束。
3. 后续如果要继续演进，最好把这个口径做成编译器配置项，而不是散落在文档约定里。

### 3.3 高优先级：减少对 AST 形状匹配的依赖，避免未来升级时脆断

当前实现的时间语义识别，其实分散在两套不同层级里：

- Ibis op 树侧：`_epoch_millis_source_op()` 通过 `Field -> relation.values[name]` 追溯规范链，见 `src/ibis_dsql/compiler.py:42-65`
- sqlglot AST 侧：`_unwrap_epoch_millis_timestamp()` 通过匹配 `CAST(FROM_UNIXTIME(arg / 1000) AS TIMESTAMP)` 的具体形状来回推原始列，见 `src/ibis_dsql/compiler.py:420-464`

这说明同一条业务语义目前被“重复编码”了两次。

短期它是有效的，但长期维护上有两个风险：

1. 上游 Ibis / sqlglot 的输出形状只要稍微变一点，某一侧识别就可能失效。
2. 语义规则分散在两个树形结构里，后续很难一眼看清“为什么这里回到 long，那里恢复成 timestamp”。

更稳的方向是：

- 尽量在 Ibis 侧就把“这是 epoch-ms 规范链”标成显式语义
- 编译阶段沿着这个标记传递，而不是到 sqlglot AST 再反向猜测
- 如果暂时不想做大改，也建议至少把“识别”和“恢复/降级”逻辑抽成一个独立 temporal policy 模块，而不是分散在 compiler 的多个 helper 里

### 3.4 中优先级：把“时间语义恢复”从手工枚举 visitor，收敛成集中策略

当前为了在非比较场景恢复 timestamp 语义，代码手工覆写了一串 visitor：

- `visit_Cast`：`src/ibis_dsql/compiler.py:536-543`
- `visit_Date`：`src/ibis_dsql/compiler.py:612-614`
- `visit_Strftime`：`src/ibis_dsql/compiler.py:616-618`
- `visit_ExtractEpochSeconds` / `visit_ExtractYear` / `visit_ExtractMonth` / `visit_ExtractDay` / `visit_ExtractHour` / `visit_ExtractMinute` / `visit_ExtractSecond`：`src/ibis_dsql/compiler.py:620-646`
- `visit_TimestampTruncate`：`src/ibis_dsql/compiler.py:648-652`

这类实现并不是错，但它有一个典型维护问题：每新增一个时间算子，都要记得把它接进这套恢复链，否则就会出现“有的时间函数对 epoch-ms 生效，有的不生效”的隐性漂移。

一个明显的信号是：

- 代码里已经有 `visit_ExtractEpochSeconds()`，但当前测试矩阵并没有把它列入已覆盖场景
- `visit_Cast()` 也专门处理了 `timestamp -> date/time`，但这条路径没有单独的时间专题测试

相关证据：

- `src/ibis_dsql/compiler.py:620-622`
- `src/ibis_dsql/compiler.py:540-541`
- `docs/testing/time-handling-test-matrix.md:39-46`
- `docs/testing/time-handling-test-matrix.md:106-110`

建议：

1. 把“哪些 op 属于时间解释上下文”收敛成一处集中声明。
2. 补两类最容易漏的 UT：
   - `epoch_ms.cast("timestamp").epoch_seconds()`
   - `epoch_ms.cast("timestamp").cast("date")` / `cast("time")`

### 3.5 中优先级：补执行级验证，不要只停留在 SQL/AST golden

当前测试做得并不差，特别是 SQL golden 和 AST 级断言都比较完整；但文档也明确承认了还没覆盖执行级验证：

- `docs/testing/time-handling-test-matrix.md:106-110`

这在普通语法扩展里问题不大，但时间处理是例外，因为真正容易出错的地方往往不在“SQL 长得像不像”，而在“数据库执行结果是不是按预期”。

尤其是下面几类规则，仅靠字符串 golden 很难彻底放心：

- `UNIX_TIMESTAMP(...) * 1000` 与目标引擎的会话时区关系
- `FROM_UNIXTIME(...)` 对 epoch 毫秒的解释口径
- `truncate("week")` 的周一起始定义
- `DATE_TRUNC('DAY', ...)` 与本地时间边界的结果值

建议新增一层轻量集成测试：

- 连接一套固定 session timezone 的目标 DSQL 环境
- 构造少量真数据
- 校验“过滤结果”“分组结果”“周起始边界结果”，而不仅仅是 SQL 字符串

这层测试不用很多，但能显著提升你对时间语义的信心。

### 3.6 中低优先级：`to_sql()` 的优化开关语义不一致，调试时间 SQL 时会增加理解成本

`to_sql()` 现在虽然保留了 `optimize` 参数，但实际总是会执行 `_optimize_sqlglot()`：

- `src/ibis_dsql/api.py:468-480`

而时间处理的一些最终形态恰恰又依赖这层 optimize 后收口，例如：

- 去掉 `EXTRACT(...)` 下的冗余 cast
- 恢复 `BETWEEN`
- 内联单层 CTE 透传壳

这些行为本身没问题，但 API 契约会让人误以为：

- `to_sql(optimize=False)` 可以看到“未优化前”的时间 SQL

实际上看不到。

这不是时间语义 bug，但会影响后续排查时间 SQL 的可解释性。建议二选一：

1. 让 `to_sql()` 真正尊重 `optimize` 参数。
2. 如果业务上确定必须始终优化，那就去掉这个参数，避免误导。

## 4. 建议实施顺序

如果只做最有价值、性价比最高的改进，我建议按下面顺序推进：

1. 先把 `epoch-ms` 规范链从普通 `cast("timestamp")` 中剥离，改成显式 helper 或显式标记。
2. 把“数据库 session timezone == 业务本地时间口径”写成正式契约，并补一条执行级校验。
3. 把时间语义识别/恢复逻辑从“多 helper + 多 visitor”收敛成一个更集中、可枚举的 temporal policy。
4. 补上缺失的时间算子 UT，尤其是 `ExtractEpochSeconds` 和 `timestamp -> date/time cast`。
5. 最后再整理 `to_sql()` 的 optimize API 契约。

## 5. 最终评价

如果只看你当前声明的两个场景，我认为这套实现已经达到了“可用且思路正确”的水平，尤其适合约束大模型生成表达式的业务模式。

但从中长期演进看，当前最需要提升的不是“再多支持几个函数”，而是把三件事做得更稳：

- 让 `epoch-ms` 规范链成为显式协议，而不是借用普通 cast
- 让“本地时间视角”成为可验证契约，而不是停留在口头假设
- 让时间语义识别逻辑从“分散、形状敏感”逐步走向“集中、语义显式”

这三件事做完以后，这套时间处理体系会从“现在能跑”明显提升到“后续更敢扩、更敢交给大模型生成”。
