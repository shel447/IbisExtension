# DSQL 时间处理测试覆盖矩阵

## 1. 目的

这份文档用于快速查看 DSQL 时间处理相关 UT 的覆盖面，重点关注两类时间入口：

- `epoch-ms bigint/int64 -> timestamp` 规范链
- 原生无时区 `timestamp` 列

测试主文件是 [test_time_handling.py](E:/code/codex_projects/IbisExtension/tests/test_time_handling.py)。

## 2. 运行方式

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_time_handling -v
```

## 3. SQL Golden 覆盖

| 场景类别 | 具体场景 | 代表测试 |
| --- | --- | --- |
| `epoch-ms` 裸投影 | 顶层直接 `select(col.cast("timestamp"))` 回写成原始 long 列 | `test_to_sql_compiles_epoch_millis_cast_to_timestamp_in_select` |
| `epoch-ms` 比较 | 与 timestamp 字面量比较时，右侧换算成毫秒 | `test_to_sql_rewrites_epoch_millis_timestamp_comparison_to_bigint` |
| `epoch-ms` 动态比较 | 与动态 timestamp 表达式比较时，右侧换算成毫秒 | `test_to_sql_rewrites_dynamic_epoch_millis_timestamp_comparison_to_bigint` |
| `epoch-ms` 区间比较 | `BETWEEN` 两侧都换算成毫秒 | `test_to_sql_rewrites_epoch_millis_timestamp_between_to_bigint` |
| `epoch-ms` 混合原生时间列比较 | 与原生无时区 `timestamp` 列双向比较 | `test_to_sql_rewrites_epoch_millis_vs_native_timestamp_column_to_bigint` / `test_to_sql_rewrites_native_timestamp_vs_epoch_millis_column_to_bigint` |
| `epoch-ms` 原生时间列区间 | `epoch-ms` 与原生 `timestamp` 下界/上界混合 `BETWEEN` | `test_to_sql_rewrites_epoch_millis_between_native_timestamp_columns_to_bigint` |
| `epoch-ms` 过滤后再投影 | `mutate` 后过滤，再把同名时间列直接投影回原始 long | `test_to_sql_supports_mutated_epoch_millis_timestamp_filter_and_select` |
| `epoch-ms` 日期过滤 | `date()` 过滤保留时间语义 | `test_to_sql_supports_mutated_epoch_millis_timestamp_date_filter` |
| `epoch-ms` 截断投影 | `truncate("D")` 投影保留时间语义 | `test_to_sql_supports_mutated_epoch_millis_timestamp_truncate_select` |
| `epoch-ms` 排序 | `order_by(cast("timestamp"))` 保留时间语义 | `test_to_sql_compiles_epoch_millis_cast_to_timestamp_in_order_by` |
| `epoch-ms` 时间算术 | `+/- INTERVAL` 保留时间语义 | `test_to_sql_preserves_interval_arithmetic_for_epoch_millis_timestamps` |
| 同名 `mutate` 比较 | `mutate(ts=ts.cast("timestamp"))` 后与 `now()/truncate()` 比较，右侧换算毫秒 | `test_to_sql_rewrites_same_name_mutated_epoch_millis_week_range_filter` |
| 同名 `mutate` 时间变换 | `date()/truncate()/strftime()` 保留时间语义 | `test_to_sql_supports_same_name_mutated_epoch_millis_temporal_transforms` |
| 同名 `mutate` 时间提取 | `year/month/day/hour/minute/second` 保留时间语义 | `test_to_sql_supports_same_name_mutated_epoch_millis_common_extracts` |
| 原生 `timestamp` 直接投影 | 直接 `select` 保持原生 SQL | `test_to_sql_leaves_native_timestamp_select_unchanged` |
| 原生 `timestamp` 排序 | `order_by` 保持原生 SQL | `test_to_sql_leaves_native_timestamp_order_by_unchanged` |
| 原生 `timestamp` 比较 | 与 timestamp 字面量比较保持原生语义 | `test_to_sql_leaves_native_timestamp_comparison_unchanged` |
| 原生 `timestamp` 时间算术 | `+/- INTERVAL` 保持原生语义 | `test_to_sql_leaves_native_timestamp_interval_arithmetic_unchanged` |
| 原生 `timestamp` 日期与截断 | `date()/truncate()` 保持原生语义 | `test_to_sql_supports_native_timestamp_date_and_truncate_select` / `test_to_sql_supports_native_timestamp_truncate_filter` |
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

## 5. 当前结论

- 直接比较场景统一落成长毫秒比较，不再生成 `CAST(... AS BIGINT)`。
- 顶层直接投影 `epoch-ms.cast("timestamp")` 时，返回给应用的是原始 long 数值。
- 一旦进入真正的时间语义上下文，例如 `date/truncate/strftime/extract/interval`，仍然会恢复为 timestamp 表达式再参与计算。
- 同名 `mutate(ts=ts.cast("timestamp"))` 已单独纳入回归，避免后续修改只覆盖“改名 mutate”而漏掉真实业务写法。

## 6. 暂未纳入本矩阵

- 带时区 `timestamp` 的正确时区换算语义
- `connect_by` 查询中的时间字段专题覆盖
- 执行引擎结果级验证
