from __future__ import annotations

import unittest

import ibis
import ibis.expr.operations as ops
import sqlglot as sg
import sqlglot.expressions as sge

from ibis_dsql.compiler import DSQLCompiler
from ibis_dsql.temporal import EpochMillisTemporalPolicy


class TemporalPolicyTest(unittest.TestCase):
    def setUp(self):
        self.compiler = DSQLCompiler()
        self.policy = EpochMillisTemporalPolicy(self.compiler)

    def test_source_op_traces_same_name_mutated_epoch_millis_field(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))

        source = self.policy.source_op(base.ts.op())

        self.assertIsInstance(source, ops.Field)
        self.assertTrue(source.dtype.is_integer())
        self.assertEqual(source.name, "ts")

    def test_build_timestamp_tags_raw_expression_for_unwrap(self):
        raw = sg.column("ts_ms", table="t0", quoted=False)

        timestamp_expr = self.policy.build_timestamp(raw, ibis.dtype("timestamp"))

        unwrapped = self.policy.unwrap_timestamp(timestamp_expr)
        self.assertIsNotNone(unwrapped)
        self.assertEqual(unwrapped.sql(), raw.sql())

    def test_unwrap_timestamp_falls_back_to_shape_matching_for_untagged_ast(self):
        raw = sg.column("ts_ms", table="t0", quoted=False)
        seconds = self.compiler.binop(sge.Div, raw.copy(), sge.convert(1000))
        expression = self.compiler.cast(
            sg.func("FROM_UNIXTIME", seconds),
            ibis.dtype("timestamp"),
        )
        expression.meta.clear()

        unwrapped = self.policy.unwrap_timestamp(expression)

        self.assertIsNotNone(unwrapped)
        self.assertEqual(unwrapped.sql(), raw.sql())

    def test_restore_timestamp_avoids_double_wrapping_tagged_expression(self):
        metrics = ibis.table([("ts_ms", "int64")], name="metrics")
        raw = sg.column("ts_ms", table="t0", quoted=False)
        timestamp_expr = self.policy.build_timestamp(raw, ibis.dtype("timestamp"))

        restored = self.policy.restore_timestamp(
            metrics.ts_ms.cast("timestamp").op(),
            timestamp_expr,
        )

        self.assertEqual(restored.sql(), timestamp_expr.sql())

