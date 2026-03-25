from __future__ import annotations

import unittest

import ibis
import sqlglot.expressions as sge

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
