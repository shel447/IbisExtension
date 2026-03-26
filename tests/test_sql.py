from __future__ import annotations

import unittest

import ibis
from ibis.backends.sql.compilers.postgres import PostgresCompiler

from ibis_dsql import UnsupportedSyntaxException
from ibis_dsql import compile as compile_expr
from ibis_dsql import to_sql
from ibis_dsql.dialect import DSQLDialect


class SqlTest(unittest.TestCase):
    def test_to_sql_supports_order_limit_queries(self):
        users = ibis.table(
            [("id", "int64"), ("name", "string"), ("score", "int64")], name="users"
        )
        expr = users.filter(users.score > 0).order_by(users.name.desc()).limit(5)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.id, t0.name, t0.score FROM users AS t0 WHERE t0.score > 0 ORDER BY t0.name DESC NULLS LAST LIMIT 5",
        )

    def test_to_sql_rewrites_startswith_literal_to_like(self):
        users = ibis.table([("name", "string")], name="users")
        expr = users.filter(users.name.startswith("ab"))

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT t0.name FROM users AS t0 WHERE t0.name LIKE 'ab%'")

    def test_to_sql_rewrites_endswith_literal_to_like(self):
        users = ibis.table([("name", "string")], name="users")
        expr = users.filter(users.name.endswith("yz"))

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT t0.name FROM users AS t0 WHERE t0.name LIKE '%yz'")

    def test_to_sql_rewrites_string_concat_to_concat_function(self):
        strings = ibis.table([("a", "string"), ("b", "string")], name="strings")
        expr = strings.select((strings.a + strings.b).name("c"))

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT CONCAT(t0.a, t0.b) AS c FROM strings AS t0")

    def test_to_sql_rewrites_nested_string_concat_to_nested_concat_function(self):
        strings = ibis.table([("a", "string"), ("b", "string")], name="strings")
        expr = strings.select((strings.a + strings.b + ibis.literal("x")).name("c"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CONCAT(CONCAT(t0.a, t0.b), 'x') AS c FROM strings AS t0",
        )

    def test_to_sql_rewrites_timestamp_string_concat_to_concat_function(self):
        expr = ibis.timestamp(ibis.now().cast("date").strftime("%Y-%m-%d") + " 09:00:00")

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(CONCAT(TO_CHAR(CAST(CURRENT_TIMESTAMP AS DATE), 'YYYY-MM-DD'), ' 09:00:00') AS TIMESTAMP) AS Cast(StringConcat((Strftime(Cast(TimestampNow(), date), '%Y-%m-%d'), ' 09:00:00')), timestamp)",
        )
        self.assertNotIn("||", sql)

    def test_to_sql_rejects_dynamic_startswith_patterns(self):
        users = ibis.table([("name", "string"), ("prefix", "string")], name="users")
        expr = users.filter(users.name.startswith(users.prefix))

        with self.assertRaisesRegex(
            UnsupportedSyntaxException,
            "DSQL does not support dynamic startswith patterns",
        ):
            to_sql(expr)

    def test_to_sql_rejects_dynamic_endswith_patterns(self):
        users = ibis.table([("name", "string"), ("suffix", "string")], name="users")
        expr = users.filter(users.name.endswith(users.suffix))

        with self.assertRaisesRegex(
            UnsupportedSyntaxException,
            "DSQL does not support dynamic endswith patterns",
        ):
            to_sql(expr)

    def test_to_sql_compiles_epoch_millis_cast_to_timestamp_in_select(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.select(metrics.ts_ms.cast("timestamp").name("ts"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP) AS ts FROM metrics AS t0",
        )

    def test_to_sql_supports_mutated_epoch_millis_timestamp_filter_and_select(self):
        metrics = ibis.table([("ts_ms", "int64"), ("value", "int64")], name="metrics")
        base = metrics.mutate(ts=metrics.ts_ms.cast("timestamp"))
        expr = base.filter(base.ts >= ibis.timestamp("2026-01-01 08:00:00")).select(
            base.ts, base.value
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP) AS ts, t0.value FROM metrics AS t0 WHERE t0.ts_ms >= CAST(UNIX_TIMESTAMP(CAST('2026-01-01T08:00:00' AS TIMESTAMP)) * 1000 AS BIGINT)",
        )

    def test_to_sql_supports_mutated_epoch_millis_timestamp_date_filter(self):
        metrics = ibis.table([("ts_ms", "int64"), ("value", "int64")], name="metrics")
        base = metrics.mutate(ts=metrics.ts_ms.cast("timestamp"))
        expr = base.filter(base.ts.date() == ibis.date("2026-01-01")).select(
            base.ts, base.value
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP) AS ts, t0.value FROM metrics AS t0 WHERE DATE(CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP)) = MAKE_DATE(2026, 1, 1)",
        )

    def test_to_sql_supports_mutated_epoch_millis_timestamp_truncate_select(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        base = metrics.mutate(ts=metrics.ts_ms.cast("timestamp"))
        expr = base.select(base.ts.truncate("D").name("d"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT DATE_TRUNC('DAY', CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP)) AS d FROM metrics AS t0",
        )

    def test_to_sql_leaves_native_timestamp_select_unchanged(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.select(events.ts.name("ts2"))

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT t0.ts AS ts2 FROM events AS t0")

    def test_to_sql_leaves_native_timestamp_order_by_unchanged(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.order_by(events.ts.desc()).limit(3)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts FROM events AS t0 ORDER BY t0.ts DESC NULLS LAST LIMIT 3",
        )

    def test_to_sql_leaves_native_timestamp_interval_arithmetic_unchanged(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.select((events.ts + ibis.interval(days=-1)).name("x"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts + INTERVAL '-1' DAY AS x FROM events AS t0",
        )

    def test_to_sql_compiles_epoch_millis_cast_to_timestamp_in_order_by(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.order_by(metrics.ts_ms.cast("timestamp").desc()).limit(3)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms FROM metrics AS t0 ORDER BY CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP) DESC NULLS LAST LIMIT 3",
        )

    def test_to_sql_preserves_interval_arithmetic_for_epoch_millis_timestamps(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.select(
            (metrics.ts_ms.cast("timestamp") + ibis.interval(days=-1)).name("x")
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP) + INTERVAL '-1' DAY AS x FROM metrics AS t0",
        )

    def test_to_sql_rewrites_epoch_millis_timestamp_comparison_to_bigint(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.filter(
            metrics.ts_ms.cast("timestamp") >= ibis.timestamp("2026-01-01 08:00:00")
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms FROM metrics AS t0 WHERE t0.ts_ms >= CAST(UNIX_TIMESTAMP(CAST('2026-01-01T08:00:00' AS TIMESTAMP)) * 1000 AS BIGINT)",
        )

    def test_to_sql_rewrites_epoch_millis_timestamp_between_to_bigint(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.filter(
            metrics.ts_ms.cast("timestamp").between(
                ibis.timestamp("2026-01-01 08:00:00"),
                ibis.timestamp("2026-01-02 08:00:00"),
            )
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms FROM metrics AS t0 WHERE t0.ts_ms BETWEEN CAST(UNIX_TIMESTAMP(CAST('2026-01-01T08:00:00' AS TIMESTAMP)) * 1000 AS BIGINT) AND CAST(UNIX_TIMESTAMP(CAST('2026-01-02T08:00:00' AS TIMESTAMP)) * 1000 AS BIGINT)",
        )

    def test_to_sql_rewrites_epoch_millis_vs_native_timestamp_column_to_bigint(self):
        events = ibis.table([("ts_ms", "int64"), ("ts", "timestamp")], name="events")
        expr = events.filter(events.ts_ms.cast("timestamp") >= events.ts)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms, t0.ts FROM events AS t0 WHERE t0.ts_ms >= CAST(UNIX_TIMESTAMP(t0.ts) * 1000 AS BIGINT)",
        )

    def test_to_sql_rewrites_native_timestamp_vs_epoch_millis_column_to_bigint(self):
        events = ibis.table([("ts_ms", "int64"), ("ts", "timestamp")], name="events")
        expr = events.filter(events.ts <= events.ts_ms.cast("timestamp"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms, t0.ts FROM events AS t0 WHERE CAST(UNIX_TIMESTAMP(t0.ts) * 1000 AS BIGINT) <= t0.ts_ms",
        )

    def test_to_sql_rewrites_epoch_millis_between_native_timestamp_columns_to_bigint(self):
        events = ibis.table(
            [("ts_ms", "int64"), ("lower_ts", "timestamp"), ("upper_ts", "timestamp")],
            name="events",
        )
        expr = events.filter(
            events.ts_ms.cast("timestamp").between(events.lower_ts, events.upper_ts)
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms, t0.lower_ts, t0.upper_ts FROM events AS t0 WHERE t0.ts_ms BETWEEN CAST(UNIX_TIMESTAMP(t0.lower_ts) * 1000 AS BIGINT) AND CAST(UNIX_TIMESTAMP(t0.upper_ts) * 1000 AS BIGINT)",
        )

    def test_to_sql_rewrites_dynamic_epoch_millis_timestamp_comparison_to_bigint(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.filter(
            metrics.ts_ms.cast("timestamp")
            >= ibis.timestamp(ibis.now().cast("date").strftime("%Y-%m-%d") + " 09:00:00")
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms FROM metrics AS t0 WHERE t0.ts_ms >= CAST(UNIX_TIMESTAMP(CAST(CONCAT(TO_CHAR(CAST(CURRENT_TIMESTAMP AS DATE), 'YYYY-MM-DD'), ' 09:00:00') AS TIMESTAMP)) * 1000 AS BIGINT)",
        )

    def test_to_sql_leaves_native_timestamp_comparison_unchanged(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.filter(events.ts >= ibis.timestamp("2026-01-01 08:00:00"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts FROM events AS t0 WHERE t0.ts >= CAST('2026-01-01T08:00:00' AS TIMESTAMP)",
        )

    def test_to_sql_supports_native_timestamp_date_and_truncate_select(self):
        events = ibis.table([("ts", "timestamp"), ("value", "int64")], name="events")
        expr = events.select(
            events.ts.date().name("d"),
            events.ts.truncate("D").name("td"),
            events.value,
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT DATE(t0.ts) AS d, DATE_TRUNC('DAY', t0.ts) AS td, t0.value FROM events AS t0",
        )

    def test_to_sql_supports_native_timestamp_truncate_filter(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.filter(
            events.ts.truncate("D") >= ibis.timestamp("2026-01-01 00:00:00")
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts FROM events AS t0 WHERE DATE_TRUNC('DAY', t0.ts) >= CAST('2026-01-01T00:00:00' AS TIMESTAMP)",
        )

    def test_to_sql_rejects_timezone_aware_timestamp_in_epoch_millis_comparison(self):
        events = ibis.table(
            [("ts_ms", "int64"), ("ts_utc", ibis.dtype("timestamp('UTC')"))],
            name="events",
        )
        expr = events.filter(events.ts_ms.cast("timestamp") >= events.ts_utc)

        with self.assertRaisesRegex(
            UnsupportedSyntaxException,
            "DSQL does not support timezone-aware timestamp fields in epoch-millis comparisons",
        ):
            to_sql(expr)

    def test_to_sql_rejects_timezone_aware_timestamp_in_epoch_millis_between(self):
        events = ibis.table(
            [
                ("ts_ms", "int64"),
                ("lower_utc", ibis.dtype("timestamp('UTC')")),
                ("upper_utc", ibis.dtype("timestamp('UTC')")),
            ],
            name="events",
        )
        expr = events.filter(
            events.ts_ms.cast("timestamp").between(events.lower_utc, events.upper_utc)
        )

        with self.assertRaisesRegex(
            UnsupportedSyntaxException,
            "DSQL does not support timezone-aware timestamp fields in epoch-millis comparisons",
        ):
            to_sql(expr)

    def test_to_sql_supports_group_filter_queries(self):
        users = ibis.table([("name", "string"), ("score", "int64")], name="users")
        expr = (
            users.group_by(users.name)
            .aggregate(total=users.score.sum())
            .filter(lambda t: t.total > 10)
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t1.name, t1.total FROM (SELECT t0.name, SUM(t0.score) AS total FROM users AS t0 GROUP BY 1) AS t1 WHERE t1.total > 10",
        )

    def test_to_sql_supports_join_queries(self):
        users = ibis.table([("id", "int64"), ("name", "string")], name="users")
        orders = ibis.table([("user_id", "int64"), ("amount", "int64")], name="orders")
        expr = users.join(orders, users.id == orders.user_id).select(
            users.name, orders.amount
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t2.name, t3.amount FROM users AS t2 INNER JOIN orders AS t3 ON t2.id = t3.user_id",
        )

    def test_to_sql_emits_cte_for_reused_relations(self):
        users = ibis.table([("id", "int64"), ("score", "int64")], name="users")
        base = users.filter(users.score > 5)
        right = base.view()
        expr = base.join(right, base.id == right.id).select(
            base.id, left_score=base.score, right_score=right.score
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "WITH t1 AS (SELECT t0.id, t0.score FROM users AS t0 WHERE t0.score > 5) SELECT t3.id, t3.score AS left_score, t4.score AS right_score FROM t1 AS t3 INNER JOIN t1 AS t4 ON t3.id = t4.id",
        )

    def test_to_sql_preserves_count_star_alias(self):
        users = ibis.table([("id", "int64")], name="users")
        expr = users.aggregate(total=users.count())

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT COUNT(*) AS total FROM users AS t0")

    def test_to_sql_formats_interval_with_separate_unit(self):
        users = ibis.table([("id", "int64")], name="users")
        expr = users.select(
            (ibis.timestamp("2024-01-01 00:00:00") + ibis.interval(days=1)).name("x")
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST('2024-01-01T00:00:00' AS TIMESTAMP) + INTERVAL '1' DAY AS x FROM users AS t0",
        )

    def test_to_sql_rewrites_position_to_instr(self):
        users = ibis.table([("name", "string")], name="users")
        expr = users.select(users.name.find("abc").name("pos"))

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT INSTR(t0.name, 'abc') - 1 AS pos FROM users AS t0")

    def test_to_sql_rewrites_not_in_to_postfix_not(self):
        users = ibis.table([("id", "int64")], name="users")
        expr = users.filter(~users.id.isin([1, 2]))

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT t0.id FROM users AS t0 WHERE t0.id NOT IN (1, 2)")

    def test_to_sql_rewrites_not_like_to_postfix_not(self):
        users = ibis.table([("name", "string")], name="users")
        expr = users.filter(~users.name.like("%x%"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name FROM users AS t0 WHERE t0.name NOT LIKE '%x%'",
        )

    def test_to_sql_rewrites_not_is_null_to_is_not_null(self):
        users = ibis.table([("name", "string")], name="users")
        expr = users.filter(~users.name.isnull())

        sql = to_sql(expr)

        self.assertEqual(sql, "SELECT t0.name FROM users AS t0 WHERE t0.name IS NOT NULL")

    def test_to_sql_rewrites_not_in_subquery_to_postfix_not(self):
        users = ibis.table([("id", "int64")], name="users")
        rel = users.filter(users.id > 1)
        expr = users.filter(~users.id.isin(rel.id))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.id FROM users AS t0 WHERE t0.id NOT IN (SELECT t0.id FROM users AS t0 WHERE t0.id > 1)",
        )

    def test_to_sql_expands_top_level_star_selection(self):
        users = ibis.table([("id", "int64"), ("name", "string")], name="users")

        sql = to_sql(users)

        self.assertEqual(sql, "SELECT users.id, users.name FROM users")

    def test_to_sql_rejects_scalar_subquery_in_where_clause(self):
        users = ibis.table([("id", "int64"), ("name", "string")], name="users")
        scalar = users.aggregate(mx=users.id.max()).mx.as_scalar()
        expr = users.filter(users.id > scalar)

        with self.assertRaisesRegex(
            UnsupportedSyntaxException,
            "DSQL does not support scalar subqueries",
        ):
            to_sql(expr)

    def test_to_sql_rejects_scalar_subquery_in_select_list(self):
        users = ibis.table([("id", "int64"), ("name", "string")], name="users")
        scalar = users.aggregate(mx=users.id.max()).mx.as_scalar()
        expr = users.select(mx=scalar)

        with self.assertRaisesRegex(
            UnsupportedSyntaxException,
            "DSQL does not support scalar subqueries",
        ):
            to_sql(expr)

    def test_to_sql_uses_dsql_float_type_names(self):
        users = ibis.table([("id", "int64")], name="users")
        expr = users.select(
            users.id.cast("float32").name("f32"),
            users.id.cast("float64").name("f64"),
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(t0.id AS FLOAT) AS f32, CAST(t0.id AS DOUBLE) AS f64 FROM users AS t0",
        )

    def test_compile_optimize_reparses_compiled_sql(self):
        users = ibis.table([("id", "int64")], name="users")
        expr = users.filter(ibis.literal(True) & (users.id > 1))

        optimized = compile_expr(expr, optimize=True, schema={"users": {"id": "BIGINT"}})

        self.assertEqual(
            optimized.sql(dialect=DSQLDialect),
            "SELECT t0.id AS id FROM users AS t0 WHERE t0.id > 1",
        )

    def test_dsql_overrides_postgres_function_and_type_output(self):
        users = ibis.table([("id", "int64")], name="users")
        cast_expr = users.select(users.id.cast("string").name("id_text"))

        self.assertEqual(
            to_sql(cast_expr),
            "SELECT CAST(t0.id AS STRING) AS id_text FROM users AS t0",
        )

        postgres_sql = PostgresCompiler().to_sqlglot(ibis.random()).sql(dialect="postgres")

        self.assertEqual(postgres_sql, 'SELECT RANDOM() AS "RandomScalar()"')
        self.assertEqual(to_sql(ibis.random()), "SELECT RAND() AS RandomScalar()")
