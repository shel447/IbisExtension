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

### 2.2 本次新增或调整能力

| 编号 | 项目 | 目标状态 | 扩展层 | 说明 |
| --- | --- | --- | --- | --- |
| N-01 | `startsWith` 字面量前缀匹配 | 新增 | Compiler | 输出 `LIKE 'x%'` |
| N-02 | `endsWith` 字面量后缀匹配 | 新增 | Compiler | 输出 `LIKE '%x'` |
| N-03 | `count(*)` 别名保留 | 回归验证 | Compiler | 当前基线已支持，增加测试锁定行为 |
| N-04 | `INTERVAL '1 DAY'` 改写为 `INTERVAL '1' DAY` | 新增 | Generator | 调整 `SINGLE_STRING_INTERVAL` 策略 |
| N-05 | `POSITION(x IN y)` 改写为 `INSTR(y, x)` | 新增 | Generator | 保持 Ibis `find()` 的 0 基语义 |
| N-06 | `NOT (a IN b)` 改写为 `a NOT IN b` | 新增 | Generator | 兼容 `b` 为子查询 |
| N-07 | `NOT (a LIKE b)` 改写为 `a NOT LIKE b` | 新增 | Generator | 仅改写 `LIKE` 形态 |
| N-08 | `NOT (a IS NULL)` 改写为 `a IS NOT NULL` | 新增 | Generator | 与已有 `notnull()` 输出保持一致 |
| N-09 | `SELECT *` 展开为显式列清单 | 新增 | Compiler | 包括顶层表表达式与星号投影场景 |
| N-10 | `REAL` 输出为 `FLOAT` | 新增 | Generator | DSQL 类型映射 |
| N-11 | `DOUBLE PRECISION` 输出为 `DOUBLE` | 新增 | Generator | DSQL 类型映射 |
| N-12 | 禁止所有标量子查询 | 新增 | Compiler | 在任何需要单值的位置统一报错 |

### 2.3 待完成项

| 编号 | 项目 | 状态 | 原因 |
| --- | --- | --- | --- |
| P-01 | `startsWith` / `endsWith` 动态前后缀匹配 | 待完成 | DSQL 不支持 `CONCAT`，也不支持 `||`，当前未提供合法的动态模式串拼接语法 |

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
- 动态前后缀场景不在本次实现范围内；若在编译阶段识别到动态表达式，则保持当前基线行为或在后续版本中再补专门策略。该点在设计文档中标记为待完成，不在本轮承诺交付。

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
- 一旦检测到 `ops.ScalarSubquery`，立即抛出 `UnsupportedOperationError`。
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

### 8.2 异常断言

新增以下失败用例：

- `WHERE a > (SELECT ...)`
- `SELECT (subquery) AS x`

期望统一抛出 `UnsupportedOperationError`，消息为 `DSQL does not support scalar subqueries`。

### 8.3 对照回归

为避免误判需求已满足或被未来变更覆盖，保留至少一组 PostgreSQL 对照：

- `POSITION(...)` 与 `INSTR(...)` 的输出差异
- `INTERVAL '1 DAY'` 与 `INTERVAL '1' DAY` 的输出差异
- `DOUBLE PRECISION` 与 `DOUBLE` 的输出差异

## 9. 待完成项说明

### 9.1 动态前后缀匹配

当前 DSQL 不支持 `CONCAT`，也不支持 `||`，因此无法把：

- `col.startswith(expr)`
- `col.endswith(expr)`

合法地转换成动态 `LIKE` 模式串。本轮不采用伪实现、不回退为不符合 DSQL 约束的写法，也不把该问题隐藏在 generator 的文本替换中。

后续若 DSQL 明确提供动态模式拼接能力，应在本节状态更新后再补实现与测试。

## 10. 结论

本次 DSQL 语法扩展采用“Compiler 处理结构和禁用能力，Generator 处理输出形态”的分层方案。该方案与现有双层继承结构一致，能以最小覆写实现目标语法，同时把无合法 DSQL 写法支撑的动态前后缀匹配明确记录为待完成项，保证设计边界清晰、行为可测试、后续扩展可持续。
