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
            "SELECT t0.ts_ms AS ts FROM metrics AS t0",
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
            "SELECT t0.ts_ms AS ts_ms, t0.value FROM metrics AS t0 WHERE t0.ts_ms >= (UNIX_TIMESTAMP('2026-01-01 08:00:00') * 1000)",
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
            "SELECT t0.ts_ms AS ts_ms, t0.value FROM metrics AS t0 WHERE DATE_TRUNC('DAY', CAST(FROM_UNIXTIME(CAST(t0.ts_ms AS DOUBLE) / 1000) AS TIMESTAMP)) = '2026-01-01'",
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

    def test_to_sql_rewrites_same_name_mutated_epoch_millis_week_range_filter(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.filter(
            (base.ts >= ibis.now().truncate("week")) & (base.ts < ibis.now())
        ).select(base.name)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t2.name FROM (SELECT t1.ts, t1.name FROM (SELECT t0.ts AS ts, t0.name FROM alarm AS t0) AS t1 WHERE t1.ts >= (UNIX_TIMESTAMP(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP - INTERVAL '1' DAY) + INTERVAL '1' DAY) * 1000) AND t1.ts < (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) * 1000)) AS t2",
        )

    def test_to_sql_supports_same_name_mutated_epoch_millis_temporal_transforms(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.select(
            base.ts.date().name("d"),
            base.ts.truncate("W").name("tw"),
            base.ts.truncate("M").name("tm"),
            base.ts.strftime("%Y-%m-%d").name("s"),
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT DATE_TRUNC('DAY', CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS d, DATE_TRUNC('WEEK', CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP) - INTERVAL '1' DAY) + INTERVAL '1' DAY AS tw, DATE_TRUNC('MONTH', CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS tm, TO_CHAR(CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP), 'YYYY-MM-DD') AS s FROM alarm AS t0",
        )

    def test_to_sql_supports_same_name_mutated_epoch_millis_common_extracts(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.select(
            base.ts.year().name("y"),
            base.ts.month().name("m"),
            base.ts.day().name("dd"),
            base.ts.hour().name("hh"),
            base.ts.minute().name("mi"),
            base.ts.second().name("ss"),
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT EXTRACT(year FROM CAST(FROM_UNIXTIME(CAST(t1.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS y, EXTRACT(month FROM CAST(FROM_UNIXTIME(CAST(t1.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS m, EXTRACT(day FROM CAST(FROM_UNIXTIME(CAST(t1.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS dd, EXTRACT(hour FROM CAST(FROM_UNIXTIME(CAST(t1.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS hh, EXTRACT(minute FROM CAST(FROM_UNIXTIME(CAST(t1.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS mi, CAST(FLOOR(EXTRACT('second' FROM CAST(FROM_UNIXTIME(CAST(t1.ts AS DOUBLE) / 1000) AS TIMESTAMP))) AS INT) AS ss FROM (SELECT t0.ts AS ts, t0.name FROM alarm AS t0) AS t1",
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
            "SELECT t0.ts_ms FROM metrics AS t0 WHERE t0.ts_ms >= (UNIX_TIMESTAMP('2026-01-01 08:00:00') * 1000)",
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
            "SELECT t0.ts_ms FROM metrics AS t0 WHERE t0.ts_ms BETWEEN UNIX_TIMESTAMP('2026-01-01 08:00:00') * 1000 AND UNIX_TIMESTAMP('2026-01-02 08:00:00') * 1000",
        )

    def test_to_sql_rewrites_epoch_millis_vs_native_timestamp_column_to_bigint(self):
        events = ibis.table([("ts_ms", "int64"), ("ts", "timestamp")], name="events")
        expr = events.filter(events.ts_ms.cast("timestamp") >= events.ts)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms, t0.ts FROM events AS t0 WHERE t0.ts_ms >= (UNIX_TIMESTAMP(t0.ts) * 1000)",
        )

    def test_to_sql_rewrites_native_timestamp_vs_epoch_millis_column_to_bigint(self):
        events = ibis.table([("ts_ms", "int64"), ("ts", "timestamp")], name="events")
        expr = events.filter(events.ts <= events.ts_ms.cast("timestamp"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts_ms, t0.ts FROM events AS t0 WHERE (UNIX_TIMESTAMP(t0.ts) * 1000) <= t0.ts_ms",
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
            "SELECT t0.ts_ms, t0.lower_ts, t0.upper_ts FROM events AS t0 WHERE t0.ts_ms BETWEEN UNIX_TIMESTAMP(t0.lower_ts) * 1000 AND UNIX_TIMESTAMP(t0.upper_ts) * 1000",
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
            "SELECT t0.ts_ms FROM metrics AS t0 WHERE t0.ts_ms >= (UNIX_TIMESTAMP(CAST(CONCAT(TO_CHAR(CAST(CURRENT_TIMESTAMP AS DATE), 'YYYY-MM-DD'), ' 09:00:00') AS TIMESTAMP)) * 1000)",
        )

    def test_to_sql_leaves_native_timestamp_comparison_unchanged(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.filter(events.ts >= ibis.timestamp("2026-01-01 08:00:00"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts FROM events AS t0 WHERE t0.ts >= '2026-01-01 08:00:00'",
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
            "SELECT DATE_TRUNC('DAY', t0.ts) AS d, DATE_TRUNC('DAY', t0.ts) AS td, t0.value FROM events AS t0",
        )

    def test_to_sql_supports_native_timestamp_common_extracts(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.select(
            events.ts.year().name("y"),
            events.ts.month().name("m"),
            events.ts.day().name("dd"),
            events.ts.hour().name("hh"),
            events.ts.minute().name("mi"),
            events.ts.second().name("ss"),
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT EXTRACT(year FROM t0.ts) AS y, EXTRACT(month FROM t0.ts) AS m, EXTRACT(day FROM t0.ts) AS dd, EXTRACT(hour FROM t0.ts) AS hh, EXTRACT(minute FROM t0.ts) AS mi, CAST(FLOOR(EXTRACT('second' FROM t0.ts)) AS INT) AS ss FROM events AS t0",
        )

    def test_to_sql_supports_native_timestamp_truncate_filter(self):
        events = ibis.table([("ts", "timestamp")], name="events")
        expr = events.filter(
            events.ts.truncate("D") >= ibis.timestamp("2026-01-01 00:00:00")
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts FROM events AS t0 WHERE DATE_TRUNC('DAY', t0.ts) >= '2026-01-01 00:00:00'",
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
        projection = compiled.expressions[0]
        self.assertIsInstance(projection, sge.Alias)
        self.assertEqual(projection.alias, "ts")
        self.assertIsInstance(projection.this, sge.Column)
        self.assertEqual(projection.this.this.name, "ts_ms")
        self.assertEqual(projection.this.table, "t0")
        self.assertIsNone(compiled.find(sge.Cast))
        self.assertIsNone(compiled.find(sge.UnixToTime))

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
        self.assertIsInstance(comparison.expression, sge.Paren)
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
        self.assertIsInstance(comparison.expression, sge.Paren)
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
        self.assertIsInstance(projection.this, sge.Column)
        self.assertEqual(projection.this.this.name, "ts_ms")
        self.assertEqual(projection.this.table, "t0")

    def test_compile_rewrites_same_name_mutated_epoch_millis_week_range_filter(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.filter(
            (base.ts >= ibis.now().truncate("week")) & (base.ts < ibis.now())
        ).select(base.name)

        compiled = compile_expr(expr)
        greater_equal = compiled.find(sge.GTE)
        less = compiled.find(sge.LT)

        self.assertIsNotNone(greater_equal)
        self.assertIsInstance(greater_equal.this, sge.Column)
        self.assertEqual(greater_equal.this.this.name, "ts")
        self.assertEqual(greater_equal.this.table, "t1")
        self.assertIsInstance(greater_equal.expression, sge.Paren)
        self.assertIsInstance(greater_equal.expression.this, sge.Mul)
        unix_timestamp = greater_equal.expression.this.this
        self.assertIsInstance(unix_timestamp, sge.Anonymous)
        self.assertEqual(unix_timestamp.name.upper(), "UNIX_TIMESTAMP")
        monday_week_start = unix_timestamp.expressions[0]
        self.assertIsInstance(monday_week_start, sge.Add)
        self.assertIsInstance(monday_week_start.this, sge.TimestampTrunc)
        self.assertIsInstance(monday_week_start.this.this, sge.Sub)

        self.assertIsNotNone(less)
        self.assertIsInstance(less.this, sge.Column)
        self.assertEqual(less.this.this.name, "ts")
        self.assertEqual(less.this.table, "t1")
        self.assertIsInstance(less.expression, sge.Paren)
        self.assertIsInstance(less.expression.this, sge.Mul)

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
