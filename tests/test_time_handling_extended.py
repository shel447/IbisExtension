from __future__ import annotations

import unittest

import ibis

from ibis_dsql import to_sql


class TimeSqlExtendedTest(unittest.TestCase):
    def test_to_sql_supports_same_name_mutated_epoch_millis_epoch_seconds(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.select(base.ts.epoch_seconds().name("s"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT EXTRACT('epoch' FROM CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS s FROM alarm AS t0",
        )

    def test_to_sql_supports_same_name_mutated_epoch_millis_cast_date_and_time(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.select(
            base.ts.cast("date").name("d"),
            base.ts.cast("time").name("t"),
        )

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP) AS DATE) AS d, CAST(CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP) AS TIME) AS t FROM alarm AS t0",
        )

    def test_to_sql_supports_same_name_mutated_epoch_millis_order_by(self):
        alarm = ibis.table([("ts", "int64"), ("name", "string")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.order_by(base.ts.desc()).limit(3)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.ts, t0.name FROM alarm AS t0 ORDER BY CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP) DESC NULLS LAST LIMIT 3",
        )

    def test_to_sql_supports_same_name_mutated_epoch_millis_interval_arithmetic(self):
        alarm = ibis.table([("ts", "int64")], name="alarm")
        base = alarm.mutate(ts=alarm.ts.cast("timestamp"))
        expr = base.select((base.ts + ibis.interval(days=-1)).name("x"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP) + INTERVAL '-1' DAY AS x FROM alarm AS t0",
        )
