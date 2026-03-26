from __future__ import annotations

import unittest

import ibis
import sqlglot.expressions as sge

from ibis_dsql import UnsupportedSyntaxException
from ibis_dsql import compile as compile_expr
from ibis_dsql import to_sql


class TimeSqlTest(unittest.TestCase):
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
        base = metrics.mutate(ts_ms=metrics.ts_ms.cast("timestamp"))
        expr = base.filter(base.ts_ms >= ibis.timestamp("2026-01-01 08:00:00")).select(
            base.ts_ms, base.value
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP) AS ts_ms, t0.value FROM metrics AS t0 WHERE t0.ts_ms >= CAST(UNIX_TIMESTAMP(CAST('2026-01-01T08:00:00' AS TIMESTAMP)) * 1000 AS BIGINT)",
        )

    def test_to_sql_supports_mutated_epoch_millis_timestamp_date_filter(self):
        metrics = ibis.table([("ts_ms", "int64"), ("value", "int64")], name="metrics")
        base = metrics.mutate(ts_ms=metrics.ts_ms.cast("timestamp"))
        expr = base.filter(base.ts_ms.date() == ibis.date("2026-01-01")).select(
            base.ts_ms, base.value
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP) AS ts_ms, t0.value FROM metrics AS t0 WHERE DATE(CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP)) = MAKE_DATE(2026, 1, 1)",
        )

    def test_to_sql_supports_mutated_epoch_millis_timestamp_truncate_select(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        base = metrics.mutate(ts_ms=metrics.ts_ms.cast("timestamp"))
        expr = base.select(base.ts_ms.truncate("D").name("d"))

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


class TimeCompilerTest(unittest.TestCase):
    def test_compile_rewrites_epoch_millis_cast_to_from_unixtime_timestamp(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.select(metrics.ts_ms.cast("timestamp").name("ts"))

        compiled = compile_expr(expr)
        cast = compiled.find(sge.Cast)

        self.assertIsNotNone(cast)
        self.assertEqual(cast.to.this, sge.DataType.Type.TIMESTAMP)
        self.assertIsNone(compiled.find(sge.UnixToTime))

        from_unixtime = next(
            node
            for node in compiled.find_all(sge.Anonymous)
            if node.name.upper() == "FROM_UNIXTIME"
        )
        division = from_unixtime.expressions[0]

        self.assertIsInstance(division, sge.Div)
        self.assertIsInstance(division.this, sge.Column)
        self.assertEqual(division.this.this.name, "ts_ms")
        self.assertEqual(division.this.table, "t0")
        self.assertEqual(division.expression, sge.convert(1000))

    def test_compile_rewrites_epoch_millis_timestamp_comparison_to_bigint_comparison(
        self,
    ):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        expr = metrics.filter(
            metrics.ts_ms.cast("timestamp") >= ibis.timestamp("2026-01-01 08:00:00")
        )

        compiled = compile_expr(expr)
        comparison = compiled.find(sge.GTE)

        self.assertIsNotNone(comparison)
        self.assertIsInstance(comparison.this, sge.Column)
        self.assertEqual(comparison.this.this.name, "ts_ms")
        self.assertEqual(comparison.this.table, "t0")
        self.assertIsNone(compiled.find(sge.UnixToTime))
        self.assertIsInstance(comparison.expression, sge.Cast)
        self.assertEqual(comparison.expression.to.this, sge.DataType.Type.BIGINT)

        multiply = comparison.expression.this
        self.assertIsInstance(multiply, sge.Mul)
        self.assertEqual(multiply.expression, sge.convert(1000))

        unix_timestamp = multiply.this
        self.assertIsInstance(unix_timestamp, sge.Anonymous)
        self.assertEqual(unix_timestamp.name.upper(), "UNIX_TIMESTAMP")

    def test_compile_rewrites_epoch_millis_vs_native_timestamp_column_to_bigint(self):
        events = ibis.table([("ts_ms", "int64"), ("ts", "timestamp")], name="events")
        expr = events.filter(events.ts_ms.cast("timestamp") >= events.ts)

        compiled = compile_expr(expr)
        comparison = compiled.find(sge.GTE)

        self.assertIsNotNone(comparison)
        self.assertIsInstance(comparison.this, sge.Column)
        self.assertEqual(comparison.this.this.name, "ts_ms")
        self.assertEqual(comparison.this.table, "t0")
        self.assertIsInstance(comparison.expression, sge.Cast)
        self.assertEqual(comparison.expression.to.this, sge.DataType.Type.BIGINT)

        multiply = comparison.expression.this
        self.assertIsInstance(multiply, sge.Mul)
        unix_timestamp = multiply.this
        self.assertIsInstance(unix_timestamp, sge.Anonymous)
        self.assertEqual(unix_timestamp.name.upper(), "UNIX_TIMESTAMP")
        native_column = unix_timestamp.expressions[0]
        self.assertIsInstance(native_column, sge.Column)
        self.assertEqual(native_column.this.name, "ts")
        self.assertEqual(native_column.table, "t0")

    def test_compile_rewrites_mutated_epoch_millis_timestamp_filter_to_bigint(self):
        metrics = ibis.table([("ts_ms", "int64"), ("value", "int64")], name="metrics")
        base = metrics.mutate(ts_ms=metrics.ts_ms.cast("timestamp"))
        expr = base.filter(base.ts_ms >= ibis.timestamp("2026-01-01 08:00:00")).select(
            base.ts_ms, base.value
        )

        compiled = compile_expr(expr)
        comparison = compiled.find(sge.GTE)
        projection = next(
            node
            for node in compiled.expressions
            if isinstance(node, sge.Alias) and node.alias == "ts_ms"
        )

        self.assertIsNotNone(comparison)
        self.assertIsInstance(comparison.this, sge.Column)
        self.assertEqual(comparison.this.this.name, "ts_ms")
        self.assertEqual(comparison.this.table, "t0")
        self.assertIsInstance(projection.this, sge.Cast)
        self.assertEqual(projection.this.to.this, sge.DataType.Type.TIMESTAMP)

    def test_compile_rejects_timezone_aware_timestamp_in_epoch_millis_comparison(self):
        events = ibis.table(
            [("ts_ms", "int64"), ("ts_utc", ibis.dtype("timestamp('UTC')"))],
            name="events",
        )
        expr = events.filter(events.ts_ms.cast("timestamp") >= events.ts_utc)

        with self.assertRaisesRegex(
            UnsupportedSyntaxException,
            "DSQL does not support timezone-aware timestamp fields in epoch-millis comparisons",
        ):
            compile_expr(expr)
