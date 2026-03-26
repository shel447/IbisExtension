from __future__ import annotations

import unittest

import ibis
import sqlglot.expressions as sge

from ibis_dsql import UnsupportedSyntaxException
from ibis_dsql import compile as compile_expr


class CompilerTest(unittest.TestCase):
    def test_compile_returns_ast_with_expected_cast_shape(self):
        users = ibis.table([("id", "int64")], name="users")
        expr = users.select(users.id.cast("string").name("id_text"))

        compiled = compile_expr(expr)
        cast = compiled.find(sge.Cast)
        column = cast.this

        self.assertIsInstance(cast, sge.Cast)
        self.assertIsInstance(column, sge.Column)
        self.assertEqual(column.this.name, "id")
        self.assertEqual(column.table, "t0")
        self.assertEqual(cast.to.this, sge.DataType.Type.VARCHAR)

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
        base = metrics.mutate(ts=metrics.ts_ms.cast("timestamp"))
        expr = base.filter(base.ts >= ibis.timestamp("2026-01-01 08:00:00")).select(
            base.ts, base.value
        )

        compiled = compile_expr(expr)
        comparison = compiled.find(sge.GTE)
        projection = next(
            node for node in compiled.expressions if isinstance(node, sge.Alias) and node.alias == "ts"
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
