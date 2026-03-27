# DSQL 时间处理测试覆盖矩阵

## 1. 目的

这份文档用于快速查看 DSQL 时间处理相关 UT 的覆盖面，重点关注两类时间入口：

- `epoch-ms bigint/int64 -> timestamp` 规范链
- 原生无时区 `timestamp` 列

测试文件包括：

- [test_time_handling.py](E:/code/codex_projects/IbisExtension/tests/test_time_handling.py)：原子能力回归
- [test_custom_time_handling_scene.py](E:/code/codex_projects/IbisExtension/tests/test_custom_time_handling_scene.py)：贴近业务写法的串联场景回归

## 2. 运行方式

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_time_handling -v
.\.venv\Scripts\python.exe -m unittest tests.test_custom_time_handling_scene -v
```

## 3. 原子能力 SQL Golden 覆盖

| 场景类别 | 具体场景 | 代表测试 |
| --- | --- | --- |
| `epoch-ms` 裸投影 | 顶层直接 `select(col.cast("timestamp"))` 回写成原始 long 列 | `test_to_sql_compiles_epoch_millis_cast_to_timestamp_in_select` |
| `epoch-ms` 比较 | 与 timestamp 字面量比较时，右侧换算成毫秒 | `test_to_sql_rewrites_epoch_millis_timestamp_comparison_to_bigint` |
| `epoch-ms` 动态比较 | 与动态 timestamp 表达式比较时，右侧换算成毫秒 | `test_to_sql_rewrites_dynamic_epoch_millis_timestamp_comparison_to_bigint` |
| `epoch-ms` 区间比较 | `BETWEEN` 两侧都换算成毫秒 | `test_to_sql_rewrites_epoch_millis_timestamp_between_to_bigint` |
| `epoch-ms` 混合原生时间列比较 | 与原生无时区 `timestamp` 列双向比较 | `test_to_sql_rewrites_epoch_millis_vs_native_timestamp_column_to_bigint` / `test_to_sql_rewrites_native_timestamp_vs_epoch_millis_column_to_bigint` |
| `epoch-ms` 原生时间列区间 | `epoch-ms` 与原生 `timestamp` 下界/上界混合 `BETWEEN` | `test_to_sql_rewrites_epoch_millis_between_native_timestamp_columns_to_bigint` |
| `epoch-ms` 过滤后再投影 | `mutate` 后过滤，再把同名时间列直接投影回原始 long | `test_to_sql_supports_mutated_epoch_millis_timestamp_filter_and_select` |
| `epoch-ms` 日期过滤 | `date()` 过滤保留时间语义，并统一输出 `DATE_TRUNC('DAY', ...)` | `test_to_sql_supports_mutated_epoch_millis_timestamp_date_filter` |
| `epoch-ms` 截断投影 | `truncate("D")` 投影保留时间语义 | `test_to_sql_supports_mutated_epoch_millis_timestamp_truncate_select` |
| `epoch-ms` 排序 | `order_by(cast("timestamp"))` 保留时间语义 | `test_to_sql_compiles_epoch_millis_cast_to_timestamp_in_order_by` |
| `epoch-ms` 时间算术 | `+/- INTERVAL` 保留时间语义 | `test_to_sql_preserves_interval_arithmetic_for_epoch_millis_timestamps` |
| 同名 `mutate` 比较 | `mutate(ts=ts.cast("timestamp"))` 后与 `now()/truncate()` 比较，右侧换算毫秒；`truncate("week")` 按周一开周 | `test_to_sql_rewrites_same_name_mutated_epoch_millis_week_range_filter` |
| 同名 `mutate` 时间变换 | `date()/truncate()/strftime()` 保留时间语义，其中 `date()` 输出 `DATE_TRUNC('DAY', ...)`，`truncate("week")` 按周一开周 | `test_to_sql_supports_same_name_mutated_epoch_millis_temporal_transforms` |
| 同名 `mutate` 时间提取 | `year/month/day/hour/minute/second` 保留时间语义 | `test_to_sql_supports_same_name_mutated_epoch_millis_common_extracts` |
| 原生 `timestamp` 直接投影 | 直接 `select` 保持原生 SQL | `test_to_sql_leaves_native_timestamp_select_unchanged` |
| 原生 `timestamp` 排序 | `order_by` 保持原生 SQL | `test_to_sql_leaves_native_timestamp_order_by_unchanged` |
| 原生 `timestamp` 比较 | 与 timestamp 字面量比较保持原生语义 | `test_to_sql_leaves_native_timestamp_comparison_unchanged` |
| 原生 `timestamp` 时间算术 | `+/- INTERVAL` 保持原生语义 | `test_to_sql_leaves_native_timestamp_interval_arithmetic_unchanged` |
| 原生 `timestamp` 日期与截断 | `date()/truncate()` 保持原生语义，其中 `date()` 也统一为 `DATE_TRUNC('DAY', ...)` | `test_to_sql_supports_native_timestamp_date_and_truncate_select` / `test_to_sql_supports_native_timestamp_truncate_filter` |
| 原生 `timestamp` 时间提取 | `year/month/day/hour/minute/second` 保持原生语义 | `test_to_sql_supports_native_timestamp_common_extracts` |
| 异常边界 | 带时区 `timestamp('UTC')` 进入 `epoch-ms` 时间优化时报错 | `test_to_sql_rejects_timezone_aware_timestamp_in_epoch_millis_comparison` / `test_to_sql_rejects_timezone_aware_timestamp_in_epoch_millis_between` |

## 4. AST 级覆盖

| 场景类别 | 断言重点 | 代表测试 |
| --- | --- | --- |
| `epoch-ms` 裸投影 | 最终 AST 里直接是原始 long 列，不再保留 timestamp cast | `test_compile_rewrites_epoch_millis_cast_to_from_unixtime_timestamp` |
| `epoch-ms` 比较 | 比较左侧回到原始 long 列，右侧是 `UNIX_TIMESTAMP(...) * 1000` | `test_compile_rewrites_epoch_millis_timestamp_comparison_to_bigint_comparison` |
| `epoch-ms` 混合原生时间列 | 原生时间列侧被换算成毫秒表达式 | `test_compile_rewrites_epoch_millis_vs_native_timestamp_column_to_bigint` |
| `mutate` 后过滤再投影 | 过滤仍走毫秒比较，最终投影仍回原始 long | `test_compile_rewrites_mutated_epoch_millis_timestamp_filter_to_bigint` |
| 同名 `mutate` 比较 | 通过 `Field -> relation.values[name]` 追溯命中 `epoch-ms` 优化 | `test_compile_rewrites_same_name_mutated_epoch_millis_week_range_filter` |
| 异常边界 | 带时区 `timestamp` 进入优化路径时直接报错 | `test_compile_rejects_timezone_aware_timestamp_in_epoch_millis_comparison` |

## 5. Custom 场景覆盖

### 5.1 场景矩阵

| 列类型 | 场景 | 预期编译策略 | 对应测试名 |
| --- | --- | --- | --- |
| `epoch-ms mutate timestamp` | `date from parts` 作为过滤下界 | 常量部件直接在编译期折叠为 `'YYYY-MM-DD'`，再通过 `UNIX_TIMESTAMP(...) * 1000` 换算成毫秒比较；动态部件才回退到 SQL 拼接 | `test_date_from_parts_of_mutate_timestamp_column` |
| 原生 `timestamp` | `date from parts` 作为过滤下界 | 常量部件直接折叠为 `'YYYY-MM-DD'`，整体保持原生时间比较 | `test_date_from_parts_of_native_timestamp_column` |
| `epoch-ms mutate timestamp` | `timestamp from parts` 作为过滤下界 | 常量部件直接折叠为 `'YYYY-MM-DD HH:MM:SS'`，再换算成毫秒，左侧回到原始 long 列 | `test_timestamp_from_parts_of_mutate_timestamp_column` |
| 原生 `timestamp` | `timestamp from parts` 作为过滤下界 | 常量部件直接折叠为 `'YYYY-MM-DD HH:MM:SS'`，整体保持原生时间比较 | `test_timestamp_from_parts_of_native_timestamp_column` |
| `epoch-ms mutate timestamp` | `strftime -> timestamp` 构造时间字符串再比较 | 先保留字符串到 timestamp 的时间语义，再在比较处整体换算成毫秒 | `test_time_to_string_of_mutate_timestamp_column` |
| 原生 `timestamp` | `strftime -> timestamp` 构造时间字符串再比较 | 保持原生 `CAST(TO_CHAR(...) AS TIMESTAMP)` 比较 | `test_time_to_string_of_native_timestamp_column` |
| `epoch-ms mutate timestamp` | `extract hour + group by` | `hour()` 先恢复为 timestamp 再 `EXTRACT`，过滤仍走毫秒比较 | `test_extract_hour_of_mutate_timestamp_column` |
| 原生 `timestamp` | `extract hour + group by` | 原生列直接 `EXTRACT(hour FROM ts)` | `test_extract_hour_of_native_timestamp_column` |
| `epoch-ms mutate timestamp` | `between` | 两个边界都换算成毫秒，不再出现 `CAST(... AS BIGINT)` | `test_time_between_of_mutate_timestamp_column` |
| 原生 `timestamp` | `between` | 保持原生 `BETWEEN timestamp AND timestamp` | `test_time_between_of_native_timestamp_column` |
| `epoch-ms mutate timestamp` | `rolling N days + group by date` | 过滤条件两端换算成毫秒，`date()` 分组前恢复为 timestamp，并输出 `DATE_TRUNC('DAY', ...)` | `test_truncate_date_of_mutate_timestamp_column` |
| 原生 `timestamp` | `rolling N days + group by date` | 保持原生时间比较，分组统一输出 `DATE_TRUNC('DAY', ts)` | `test_truncate_date_of_native_timestamp_column` |
| `epoch-ms mutate timestamp` | `truncate week range` | `truncate(\"week\")` 按周一开周生成，再换算成毫秒后参与比较 | `test_truncate_week_of_mutate_timestamp_column` |
| 原生 `timestamp` | `truncate week range` | `truncate(\"week\")` 按周一开周生成，再保持原生时间比较 | `test_truncate_week_of_native_timestamp_column` |

### 5.2 这批 custom 用例主要验证什么

- 它们不是替代原子 UT，而是验证大模型常见生成路径在“过滤 + mutate + group by + 聚合 + order by”串联后仍然保持正确时间语义。
- 其中 `date from parts`、`rolling N days + group by date` 暴露的是编译器真实语义问题，已经通过时间语义恢复和毫秒化简修正。
- 其余部分用例同时锁定了 DSQL 时间字面量格式，确保最终 SQL 对完整日期/时间字符串直接输出 `'YYYY-MM-DD'` / `'YYYY-MM-DD HH:MM:SS'`，中间不出现 `'T'`，也不额外包显式 `CAST`。

## 6. 当前结论

- 直接比较场景统一落成长毫秒比较，不再生成 `CAST(... AS BIGINT)`。
- 顶层直接投影 `epoch-ms.cast("timestamp")` 时，返回给应用的是原始 long 数值。
- 一旦进入真正的时间语义上下文，例如 `date/truncate/strftime/extract/interval`，仍然会恢复为 timestamp 表达式再参与计算。
- 同名 `mutate(ts=ts.cast("timestamp"))` 已单独纳入回归，避免后续修改只覆盖“改名 mutate”而漏掉真实业务写法。
- custom 场景已覆盖从“时间部件构造”到“日期分组/周截断过滤”的常见大模型生成路径，可直接用这份矩阵判断某类场景是否已有回归。

## 7. 暂未纳入本矩阵

- 带时区 `timestamp` 的正确时区换算语义
- `connect_by` 查询中的时间字段专题覆盖
- 执行引擎结果级验证
