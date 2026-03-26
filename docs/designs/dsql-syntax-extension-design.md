# DSQL 语法扩展设计说明

## 1. 文档目的

本文档用于说明 `ibis-framework==11.0.0` 与 `sqlglot==28.1.0` 组合下，`ibis-dsql` 项目针对 DSQL 方言新增和调整的语法扩展设计。本文档是正式设计说明，不是实施计划；重点描述扩展范围、分层策略、实现落点、异常边界和测试验收口径。

## 2. 扩展语法与算子清单

### 2.1 已有基线能力

| 编号 | 项目 | 当前状态 | 说明 |
| --- | --- | --- | --- |
| B-01 | 标识符默认不加引号 | 已支持 | 由 DSQL generator 控制 |
| B-02 | `RANDOM()` 输出为 `RAND()` | 已支持 | 由 DSQL generator 覆写 |
| B-03 | `VARCHAR` / `TEXT` 输出为 `STRING` | 已支持 | 由 DSQL generator `TYPE_MAPPING` 控制 |
| B-04 | 普通字符串拼接输出 `CONCAT(...)` | 已支持 | 由 DSQL generator 将 `||` 序列化为 `CONCAT(...)` |

### 2.2 本次新增或调整能力

| 编号 | 项目 | 当前状态 | 扩展层 | 说明 |
| --- | --- | --- | --- | --- |
| N-01 | `startsWith` 字面量前缀匹配 | 已实现 | Compiler | 输出 `LIKE 'x%'` |
| N-02 | `endsWith` 字面量后缀匹配 | 已实现 | Compiler | 输出 `LIKE '%x'` |
| N-03 | `count(*)` 别名保留 | 已锁定 | Compiler | 基线已支持，已通过回归测试锁定行为 |
| N-04 | `INTERVAL '1 DAY'` 改写为 `INTERVAL '1' DAY` | 已实现 | Generator | 通过 `SINGLE_STRING_INTERVAL=False` 生效 |
| N-05 | `POSITION(x IN y)` 改写为 `INSTR(y, x)` | 已实现 | Generator | 保持 Ibis `find()` 的 0 基语义 |
| N-06 | `NOT (a IN b)` 改写为 `a NOT IN b` | 已实现 | Generator | 兼容右侧为子查询 |
| N-07 | `NOT (a LIKE b)` 改写为 `a NOT LIKE b` | 已实现 | Generator | 仅改写 `LIKE` 形态 |
| N-08 | `NOT (a IS NULL)` 改写为 `a IS NOT NULL` | 已实现 | Generator | 与已有 `notnull()` 输出保持一致 |
| N-09 | `SELECT *` 展开为显式列清单 | 已实现 | Compiler | 包括顶层表表达式与星号投影场景 |
| N-10 | `REAL` 输出为 `FLOAT` | 已实现 | Generator | DSQL 类型映射 |
| N-11 | `DOUBLE PRECISION` 输出为 `DOUBLE` | 已实现 | Generator | DSQL 类型映射 |
| N-12 | 禁止所有标量子查询 | 已实现 | Compiler | 在任何需要单值的位置统一报错 |
| N-13 | `CONNECT BY` / `LEVEL` 层次查询 | 已实现 | API + Compiler | 通过 `connect_by()` helper 和内部 `__connect` lowering 实现 |
| N-14 | `epoch-ms bigint -> timestamp` 与原生时间列混合比较协议 | 已实现 | Compiler | 非比较场景保留时间语义；与无时区原生 `timestamp` 混合比较时化简为 long 毫秒比较 |

### 2.3 待完成项

| 编号 | 项目 | 状态 | 原因 |
| --- | --- | --- | --- |
| P-01 | `startsWith` / `endsWith` 动态前后缀匹配 | 待完成 | 虽然 DSQL 已支持 `CONCAT`，但本轮仍不开放动态模式拼接行为，继续保持编译期显式报错 |
| P-02 | 带时区 `timestamp` 表字段时间优化 | 待完成 | 当前只支持无时区原生 `timestamp` 与 `epoch-ms` 规范链混合比较；`timestamp('UTC')` 等带时区列后续单独设计 |

## 3. 设计背景与现状分析

### 3.1 当前 DSQL 扩展分层

当前项目采用双层继承式扩展：

- `DSQLCompiler(PostgresCompiler)` 负责 Ibis 表达式到 sqlglot AST 的翻译。
- `DSQLDialect(Postgres)` 负责 sqlglot AST 到 DSQL SQL 字符串的序列化。

该分层仍然适用于本次扩展。结构级语义和不支持能力应优先在 Compiler 层处理；仅影响输出形态和关键字排列的差异应放在 Generator 层处理。

### 3.2 基线现状

当前基线输出与目标语法存在以下偏差：

- `startsWith` 生成 `STARTS_WITH(col, 'x')`，不是 `LIKE 'x%'`。
- `endsWith` 生成 `RIGHT(col, LENGTH('x')) = 'x'`，不是 `LIKE '%x'`。
- `INTERVAL` 仍然输出为单字符串模式，如 `INTERVAL '1 DAY'`。
- `find()` 生成 `POSITION(substr IN col) - 1`，不是 `INSTR(col, substr) - 1`。
- `NOT IN` / `NOT LIKE` 当前表现为 `NOT (...)` 包裹式写法。
- 顶层表表达式和星号投影仍然可能输出 `SELECT *`。
- 标量子查询默认允许生成，例如 `WHERE a > (SELECT ...)`。
- Ibis 本身不提供递归查询表达能力，无法直接生成 DSQL 的 `CONNECT BY` 层次查询。

同时也有一条需求当前已经满足：

- `COUNT(*) AS alias` 在聚合别名场景下已经能正确输出，因此本次只需补充回归测试，不额外引入实现风险。

## 4. 总体设计原则

### 4.1 分层原则

- Compiler 层只处理三类问题：
  - Ibis 语义需要改写的表达式翻译。
  - 结构级 SQL 形态调整，例如 `SELECT *` 展开。
  - 明确不支持的语法拦截，例如标量子查询。
- Generator 层处理以下问题：
  - 同一 AST 的不同序列化风格。
  - SQL 关键字顺序和局部语法形态。
  - 类型名、函数名、字面量和 interval 输出格式。

### 4.2 设计约束

- 不修改 Ibis 或 sqlglot 上游源码。
- 尽量继承 PostgreSQL 现有行为，只覆写 DSQL 差异点。
- 对已满足的需求优先补测试，不做无意义重写。
- 对暂时无合法 DSQL 语法支撑的需求，明确登记为待完成，而不是伪造不成立的实现。

## 5. Compiler 层设计

### 5.1 `startsWith` / `endsWith` 的字面量改写

#### 5.1.1 目标

将以下字面量场景统一改写为 `LIKE`：

- `col.startswith('x')` -> `col LIKE 'x%'`
- `col.endswith('x')` -> `col LIKE '%x'`

#### 5.1.2 实现方式

- 在 `DSQLCompiler` 中覆写对应的 `visit_*` 逻辑，直接返回 sqlglot 的 `Like` 表达式。
- 对字面量参数，直接在 Compiler 中构造最终模式串字面量，不进入 `CONCAT` 或其它字符串拼接路径。
- 动态前后缀场景不在本次实现范围内；当前实现会在编译阶段直接抛出不支持异常，避免继续生成不符合 DSQL 约束的 SQL。该点在设计文档中标记为待完成，不在本轮承诺交付。

### 5.2 `SELECT *` 展开

#### 5.2.1 目标

所有最终输出 SQL 中不出现 `*`，统一替换为显式列清单。

#### 5.2.2 现有问题

当前 `SELECT *` 可能来自两条路径：

- `to_sql(table_expr)` 在根节点是表时，直接包成 `SELECT * FROM table`。
- `visit_Select()` 在 `op.is_star_selection()` 场景中显式写入 `STAR`。

#### 5.2.3 实现方式

- 在 `DSQLCompiler.to_sqlglot()` 中，若最终输出是根级 `SELECT * FROM table`，则基于表 schema 展开为显式列。
- 在 `DSQLCompiler.visit_Select()` 中，禁止在星号投影场景保留 `STAR`，统一按 parent relation 的 schema 生成列列表。
- 该策略不依赖 `sqlglot.optimize()` 把双层 `SELECT` 压扁，而是直接输出目标形态，减少不必要的额外变换。

### 5.3 标量子查询禁用

#### 5.3.1 目标

DSQL 不允许在需要单值的位置使用标量子查询，因此以下形态均应失败：

- `SELECT (subquery) AS x`
- `WHERE a > (SELECT ...)`
- 函数参数中的标量子查询
- 其它任何表达式位置的标量子查询

#### 5.3.2 实现方式

- 在 `DSQLCompiler` 中覆写 `visit_ScalarSubquery()`。
- 一旦检测到 `ops.ScalarSubquery`，立即抛出 `UnsupportedSyntaxException`。
- 异常消息固定为 `DSQL does not support scalar subqueries`，保持一致性和可测试性。

#### 5.3.3 边界

- `IN (SELECT ...)` 不属于标量子查询，保持支持。
- `EXISTS (SELECT ...)` 若未来出现，应按其自身语义单独评估，不纳入本次标量子查询禁用范围。

### 5.4 `count(*)` 别名

#### 5.4.1 目标

锁定 `COUNT(*) AS alias` 的正确输出，避免后续改动导致回归。

#### 5.4.2 实现方式

- 本轮不主动修改实现。
- 通过回归测试验证 `users.aggregate(total=users.count())` 仍输出 `COUNT(*) AS total`。
- 若测试揭示某些特殊路径会丢别名，再追加最小覆写。

### 5.5 `CONNECT BY` / `LEVEL` 取巧实现

#### 5.5.1 目标

在不修改上游 Ibis 递归语义的前提下，为 DSQL 提供可编译的层次查询入口，首期支持：

- `START WITH`
- `CONNECT BY PRIOR`
- `NOCYCLE`
- `LEVEL`

同时保持整个扩展“禁止标量子查询”的既有约束不变。

#### 5.5.2 公开入口

新增模块级 helper：

- `ibis_dsql.connect_by(table, *, start_with, parent_key, child_key, nocycle=False, level_name="level")`

其中：

- `start_with` 必须解析为布尔表达式。
- `start_with` 允许 `IN (subquery)` 和 `EXISTS (...)` 这类非标量子查询。
- `parent_key` / `child_key` 必须只引用输入表本身，首期只支持单组等值父子键。
- `level_name` 默认是 `level`；若与输入表已有列名冲突，立即报错。

#### 5.5.3 内部协议

实现不直接构造递归 AST，而是分两层：

1. 先在 Ibis 层构造一个命名为 `__connect` 的内部视图。
2. 该视图的投影中追加以下保留列：
   - `__connect_start_with`
   - `__connect_parent_key`
   - `__connect_child_key`
   - `__connect_nocycle`
   - `level_name` 对应的 `LEVEL` 占位列
3. 再在外层返回普通表表达式，只暴露源表列和 `level_name`。

这样做的作用是：

- 调用方继续拿到普通 `ibis.Table`，可继续做 `select/filter/order_by`。
- 编译器可以在 SQLGlot AST 阶段稳定识别 `__connect`，再一次性降级为真正的 `CONNECT BY`。

#### 5.5.4 Lowering 规则

`DSQLCompiler.to_sqlglot()` 在常规翻译完成后增加一个 DSQL 专用 lowering 步骤：

- 识别根查询 `FROM __connect` 且 `WITH __connect AS (...)` 的协议形态。
- 从 `__connect` CTE 中提取 `start_with`、`parent_key`、`child_key`、`nocycle` 和 `level_name`。
- 若 `__connect` 的内部查询只是简单基表投影，则直接还原为基表 `FROM`。
- 若内部查询已包含过滤或其它输入表语义，则保留成派生表子查询，避免丢失上游条件。
- 将外层对 `level_name` 的引用改写为 SQL 伪列 `LEVEL`。
- 将整个查询改写为：
  - `SELECT ... FROM ... [WHERE ...] START WITH ... CONNECT BY [NOCYCLE] PRIOR ... = ... [ORDER BY ...]`
- 去掉 `__connect` CTE 及所有 `__connect_*` 保留列，保证最终输出里不泄漏内部协议。

#### 5.5.5 `optimize=True` 策略

- `CONNECT BY` 的构造不依赖 `sqlglot.optimizer.optimize()`。
- 对命中 `CONNECT BY` lowering 的查询，先完成 lowering，再跳过通用 optimizer。
- 这样可以避免 optimizer 重新解析或重排层次查询结构时破坏 DSQL 语义。
- 对非 `CONNECT BY` 查询，仍保持现有 `optimize=True` 行为。

### 5.6 `epoch-ms bigint -> timestamp` 时间语义优化

#### 5.6.1 背景

当前业务数据表中的时间字段是 13 位 UTC 毫秒 `bigint`。为了约束大模型生成的 Ibis 表达式，调用方约定：

- 先把 long 字段 `cast("timestamp")`
- 再通过 `mutate()` 等关系级投影把它暴露成后续可复用的时间列
- 后续一律按时间类型继续写表达式
- 编译器负责把这条规范链翻译成符合 DSQL 语义且尽量高效的 SQL

本轮以这条规范链为主，同时覆盖两种真实入口：

- `int64/bigint` 字段先经 `mutate(... cast("timestamp"))` 暴露为时间列，再参与后续时间逻辑
- 表中原生无时区 `timestamp` 列直接参与后续时间逻辑

带时区时间列不纳入当前范围。

#### 5.6.2 非比较场景

当 `bigint/int64 -> timestamp` cast 出现在非比较场景时，分两类处理：

- 顶层裸投影：如果最终只是把这类时间列直接 `SELECT` 出去，不再输出 timestamp 字符串，而是回写为原始 `bigint/int64` 列
- 其余时间语义场景：继续保留“时间语义”

保留时间语义的典型场景包括：

- `ORDER BY`
- `date()` / `truncate()` / `strftime()`
- `year/month/day/hour/minute/second` 等提取函数
- 时间算术，例如 `+/- INTERVAL`
- 其它不属于比较运算、且仍然需要按时间值解释的上下文

这类场景统一输出为：

- `CAST(FROM_UNIXTIME(col / 1000) AS TIMESTAMP)`

当前 sqlglot/Postgres generator 会把 `/ 1000` 序列化成带显式 double cast 的除法形式；这不改变语义，本轮接受该输出细节。

#### 5.6.3 比较场景

当比较表达式满足以下条件时，改写成 long 毫秒比较：

- 至少一侧是 `bigint/int64 -> timestamp` 的规范 cast
- 两侧操作数都是 timestamp 语义
- 所涉及的 timestamp 操作数都不带 timezone
- 覆盖 `= != > >= < <=`
- 以及 `BETWEEN`

改写规则为：

- `epoch_ms_col.cast("timestamp") <op> ts_expr`
  -> `epoch_ms_col <op> (UNIX_TIMESTAMP(ts_expr) * 1000)`
- `epoch_ms_col.cast("timestamp").between(lower_ts, upper_ts)`
  -> `epoch_ms_col BETWEEN UNIX_TIMESTAMP(lower_ts) * 1000 AND UNIX_TIMESTAMP(upper_ts) * 1000`

如果另一侧本身也是同样的 `epoch-ms` cast，则直接还原为原始 long 列比较。
如果另一侧是无时区原生 `timestamp` 列，则将其换算为 `UNIX_TIMESTAMP(col) * 1000` 后再比较。
如果是“同名 `mutate` 暴露出来的时间列”，也沿关系值继续追溯到源 `epoch-ms` 规范 cast，确保比较优化不会因为中间 `Field` 包装而失效。
如果命中的是带时区 `timestamp`，立即抛出 `UnsupportedSyntaxException`，避免在未定义时区口径下静默生成 SQL。

#### 5.6.4 时间解释口径

- 时间字符串按本地时间格式解释，例如 `'2026-01-01 08:00:00'` 表示本地时间早上 8 点
- 由 `strftime`、字符串拼接等方式构造出的 timestamp 表达式，先按正常 timestamp 语义生成，再在比较优化中统一通过 `UNIX_TIMESTAMP(...) * 1000` 换算成 long
- 无时区原生 `timestamp` 列在混合比较中按当前会话/本地时间口径经 `UNIX_TIMESTAMP(...)` 换算成毫秒
- 带时区 `timestamp` 列本轮不参与这条优化路径，统一视为待后续专题支持

## 6. Generator 层设计

### 6.1 Interval 输出格式

#### 6.1.1 目标

将 `INTERVAL '1 DAY'` 改写为 `INTERVAL '1' DAY`。

#### 6.1.2 实现方式

- 在 `DSQLDialect.Generator` 中关闭 `SINGLE_STRING_INTERVAL`。
- 保持其它 interval 单位处理与 PostgreSQL 兼容，避免额外行为偏差。

### 6.2 `POSITION` 改写为 `INSTR`

#### 6.2.1 目标

将 sqlglot `StrPosition` 的输出从 `POSITION(substr IN col)` 改为 `INSTR(col, substr)`。

#### 6.2.2 实现方式

- 在 `DSQLDialect.Generator.TRANSFORMS` 中覆写 `exp.StrPosition` 的 transform。
- 输出形态为 `INSTR(arg, substr)`。
- 由于 Ibis `find()` 当前仍在 Compiler 层追加 `- 1`，因此最终结果保持为 `INSTR(col, 'x') - 1`，不改变现有语义。

### 6.3 `NOT` 语法重排

#### 6.3.1 目标

将 DSQL 要求的后置 `NOT` 语法收敛为以下三类：

- `a NOT IN b`
- `a NOT LIKE b`
- `a IS NOT NULL`

#### 6.3.2 实现方式

- 覆写 `DSQLDialect.Generator.not_sql()`。
- 识别 `Not` 节点的子表达式类型：
  - `exp.In` -> 输出 `lhs NOT IN rhs`
  - `exp.Like` -> 输出 `lhs NOT LIKE rhs`
  - `exp.Is` 且目标为 `NULL` -> 输出 `lhs IS NOT NULL`
- 其它 `NOT (...)` 保持基线输出，避免误改写。

#### 6.3.3 子查询支持

- 对 `NOT IN (SELECT ...)`，右侧仍然按 sqlglot 的查询表达式输出。
- 本次设计不对 `NOT EXISTS` 做额外改写。

### 6.4 类型映射

#### 6.4.1 目标

适配 DSQL 类型名：

- `REAL` -> `FLOAT`
- `DOUBLE PRECISION` -> `DOUBLE`

#### 6.4.2 实现方式

- 在 `DSQLDialect.Generator.TYPE_MAPPING` 中补充对应映射。
- 与已存在的 `VARCHAR` / `TEXT` -> `STRING` 并存，不影响现有已支持能力。

## 7. 异常与不支持语法策略

### 7.1 明确不支持项

- 所有标量子查询
- 动态 `startsWith` / `endsWith`
- `ORDER SIBLINGS BY`
- `CONNECT_BY_ROOT`
- `SYS_CONNECT_BY_PATH`
- 复合父子键
- 任意 `prior(...)` 自定义谓词 DSL

### 7.2 处理原则

- 对已知且明确不支持的能力，优先在 Compiler 层尽早报错。
- 对暂时未交付的能力，在正式设计文档中保留状态记录，避免后续误判为遗漏。
- 异常信息保持短且稳定，便于测试和上层调用方识别。

## 8. 测试与验收设计

### 8.1 Golden SQL 回归

新增或调整以下用例：

- `startsWith('ab')` -> `LIKE 'ab%'`
- `endsWith('yz')` -> `LIKE '%yz'`
- `COUNT(*) AS total`
- `INTERVAL '1' DAY`
- `INSTR(col, 'x') - 1`
- `a NOT IN (1, 2)`
- `a NOT LIKE '%x%'`
- `a IS NOT NULL`
- `a NOT IN (SELECT ...)`
- `SELECT *` 展开后的显式列清单
- `FLOAT` / `DOUBLE` 类型输出
- `START WITH ... CONNECT BY PRIOR ...`
- `NOCYCLE`
- `LEVEL` 投影
- `LEVEL > 1` 过滤
- `start_with` 中的 `IN (subquery)`
- `start_with` 中的 `EXISTS (...)`
- 派生表输入上的 `CONNECT BY`
- `optimize=True` 下的 `CONNECT BY` 稳定输出
- `mutate(ts_ms=ts_ms.cast("timestamp"))` 后继续 `filter/select`
- `mutate` 出来的时间列继续做 `date()` / `truncate()`
- 原生 `timestamp` 列的 `select/order_by/+ interval` 保持原生 SQL
- 原生 `timestamp` 列继续做 `date()` / `truncate()`
- `epoch-ms.cast("timestamp")` 与无时区原生 `timestamp` 列的双向比较
- `epoch-ms.cast("timestamp").between(native_lower, native_upper)`

### 8.2 异常断言

新增以下失败用例：

- `WHERE a > (SELECT ...)`
- `SELECT (subquery) AS x`
- `connect_by(..., start_with=non_boolean, ...)`
- `connect_by(..., parent_key=foreign_expr, ...)`
- `connect_by(..., level_name='existing_column', ...)`
- `epoch-ms.cast("timestamp")` 与 `timestamp('UTC')` 混合比较
- `epoch-ms.cast("timestamp").between(tz_lower, tz_upper)`

期望统一抛出 `UnsupportedSyntaxException`，消息为 `DSQL does not support scalar subqueries`。
对 helper 参数错误则抛出稳定的输入/类型异常。

### 8.3 对照回归

为避免误判需求已满足或被未来变更覆盖，保留至少一组 PostgreSQL 对照：

- `POSITION(...)` 与 `INSTR(...)` 的输出差异
- `INTERVAL '1 DAY'` 与 `INTERVAL '1' DAY` 的输出差异
- `DOUBLE PRECISION` 与 `DOUBLE` 的输出差异

## 9. 待完成项说明

### 9.1 动态前后缀匹配

当前 DSQL 已支持普通 `CONCAT(...)` 字符串拼接，但本轮仍不开放将其用于动态 `LIKE` 模式串构造，因此以下场景继续视为待完成项：

- `col.startswith(expr)`
- `col.endswith(expr)`

本轮不采用伪实现，也不把该问题隐藏在 generator 的文本替换中。

当前实现对动态前后缀场景直接报不支持异常，以避免把无效 SQL 传递给下游执行层。后续若 DSQL 明确提供动态模式拼接能力，应在本节状态更新后再补实现与测试。

### 9.2 带时区时间字段

本轮时间优化已经覆盖两类场景：

- `epoch-ms bigint` 先 cast 成 `timestamp`，再按时间类型写表达式
- 无时区原生 `timestamp` 与这条规范链的混合比较

当前仍然待完成的是带时区 `timestamp` 字段，例如 `timestamp('UTC')`。它们暂未纳入统一 long 化简策略，原因是：

- 需要先定义时区换算口径，不能直接复用当前无时区时间列的 `UNIX_TIMESTAMP(...)` 规则
- 若在本轮静默接入，容易在比较和边界值上引入不可见的时区偏差

因此带时区时间字段的编译优化保留到后续专题中单独设计与实现。

## 10. 结论

本次 DSQL 语法扩展继续采用“Compiler 处理结构和禁用能力，Generator 处理输出形态”的分层方案，并在此基础上新增了 `connect_by()` helper 和 `__connect` 内部 lowering 机制，用最小侵入方式补上了 Ibis 原生缺失的层次查询表达能力。针对时间语义，本轮进一步把 `epoch-ms bigint -> timestamp` 规范链与无时区原生 `timestamp` 列的混合比较一起纳入编译协议：非比较场景保留时间表达式，命中混合比较时化简成 long 毫秒比较。与此同时，动态前后缀匹配和带时区时间字段等仍未完成的能力继续明确登记为待完成项，保证设计边界清晰、行为可测试、后续扩展可持续。
