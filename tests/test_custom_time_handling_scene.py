from __future__ import annotations

import unittest

import ibis
from ibis import _, table

from ibis_dsql import to_sql

TableInt64 = table(
    schema=dict(
        name="string",
        ts="int64",
    ),
    name="TableInt64"
)
TableInt64 = TableInt64.mutate(ts=TableInt64.ts.cast("timestamp"))

TableTimestamp = table(
    schema=dict(
        name="string",
        ts="timestamp",
    ),
    name="TableTimestamp"
)

class TimeSqlTest(unittest.TestCase):
    def test_date_from_parts_of_mutate_timestamp_column(self):
        start_time = ibis.date(2026, 1, 3)
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableInt64 AS t0 WHERE t0.ts >= (UNIX_TIMESTAMP('2026-01-03') * 1000)",
        )

    def test_date_from_parts_of_native_timestamp_column(self):
        start_time = ibis.date(2026, 1, 3)
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= '2026-01-03'",
        )

    def test_timestamp_from_parts_of_mutate_timestamp_column(self):
        start_time = ibis.timestamp(2026, 1, 3, 10, 30, 0)
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableInt64 AS t0 WHERE t0.ts >= (UNIX_TIMESTAMP('2026-01-03 10:30:00') * 1000)",
        )

    def test_timestamp_from_parts_of_native_timestamp_column(self):
        start_time = ibis.timestamp(2026, 1, 3, 10, 30, 0)
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)
        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= '2026-01-03 10:30:00'",
        )

    def test_time_to_string_of_mutate_timestamp_column(self):
        yesterday = ibis.now() - ibis.interval(days=1)
        start_time = ibis.timestamp(yesterday.strftime("%Y-%m-%d"))
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableInt64 AS t0 WHERE t0.ts >= (UNIX_TIMESTAMP(CAST(TO_CHAR(CURRENT_TIMESTAMP - INTERVAL '1' DAY, 'YYYY-MM-DD') AS TIMESTAMP)) * 1000)",
        )

    def test_time_to_string_of_native_timestamp_column(self):
        yesterday = ibis.now() - ibis.interval(days=1)
        start_time = ibis.timestamp(yesterday.strftime("%Y-%m-%d"))
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts >= CAST(TO_CHAR(CURRENT_TIMESTAMP - INTERVAL '1' DAY, 'YYYY-MM-DD') AS TIMESTAMP)",
        )

    def test_extract_hour_of_mutate_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        filtered = TableInt64.filter(TableInt64["ts"] >= start_time)

        expr = (filtered.mutate(hour=filtered["ts"].hour())
                .group_by("hour")
                .aggregate(cnt=_.count())
                .order_by("hour"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT EXTRACT(hour FROM CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS hour, COUNT(*) AS cnt FROM TableInt64 AS t0 WHERE t0.ts >= (UNIX_TIMESTAMP('2026-01-28 01:00:00') * 1000) GROUP BY EXTRACT(hour FROM CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) ORDER BY hour",
        )

    def test_extract_hour_of_native_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        filtered = TableTimestamp.filter(TableTimestamp["ts"] >= start_time)

        expr = (filtered.mutate(hour=filtered["ts"].hour())
                .group_by("hour")
                .aggregate(cnt=_.count())
                .order_by("hour"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT EXTRACT(hour FROM t0.ts) AS hour, COUNT(*) AS cnt FROM TableTimestamp AS t0 WHERE t0.ts >= '2026-01-28 01:00:00' GROUP BY EXTRACT(hour FROM t0.ts) ORDER BY hour",
        )

    def test_time_between_of_mutate_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        end_time = ibis.timestamp("2026-01-28 09:00:00")
        filtered = TableInt64.filter(TableInt64.ts.between(start_time, end_time))

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableInt64 AS t0 WHERE t0.ts BETWEEN UNIX_TIMESTAMP('2026-01-28 01:00:00') * 1000 AND UNIX_TIMESTAMP('2026-01-28 09:00:00') * 1000",
        )

    def test_time_between_of_native_timestamp_column(self):
        start_time = ibis.timestamp("2026-01-28 01:00:00")
        end_time = ibis.timestamp("2026-01-28 09:00:00")
        filtered = TableTimestamp.filter(TableTimestamp.ts.between(start_time, end_time))

        expr = filtered.select([_.name, _.ts])

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name, t0.ts FROM TableTimestamp AS t0 WHERE t0.ts BETWEEN '2026-01-28 01:00:00' AND '2026-01-28 09:00:00'",
        )

    def test_truncate_date_of_mutate_timestamp_column(self):
        start_time = ibis.now().date() - ibis.interval(days=5)
        end_time = ibis.now()
        expr = (TableInt64.filter((_.ts >= start_time) & (_.ts < end_time))
                    .group_by(day=_.ts.date())
                    .aggregate(cnt=_.count())
                    .order_by("day"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT DATE_TRUNC('DAY', CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) AS day, COUNT(*) AS cnt FROM TableInt64 AS t0 WHERE t0.ts < (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) * 1000) AND t0.ts >= (UNIX_TIMESTAMP(DATE_TRUNC('DAY', CURRENT_TIMESTAMP) - INTERVAL '5' DAY) * 1000) GROUP BY DATE_TRUNC('DAY', CAST(FROM_UNIXTIME(CAST(t0.ts AS DOUBLE) / 1000) AS TIMESTAMP)) ORDER BY day",
        )

    def test_truncate_date_of_native_timestamp_column(self):
        start_time = ibis.now().date() - ibis.interval(days=5)
        end_time = ibis.now()
        expr = (TableTimestamp.filter((_.ts >= start_time) & (_.ts < end_time))
                    .group_by(day=_.ts.date())
                    .aggregate(cnt=_.count())
                    .order_by("day"))

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT DATE_TRUNC('DAY', t0.ts) AS day, COUNT(*) AS cnt FROM TableTimestamp AS t0 WHERE t0.ts < CURRENT_TIMESTAMP AND t0.ts >= (DATE_TRUNC('DAY', CURRENT_TIMESTAMP) - INTERVAL '5' DAY) GROUP BY DATE_TRUNC('DAY', t0.ts) ORDER BY day",
        )

    def test_truncate_week_of_mutate_timestamp_column(self):
        start_time = ibis.now().truncate("week")
        end_time = ibis.now()
        expr = TableInt64.filter((_.ts >= start_time) & (_.ts < end_time)).select(_.name)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name FROM TableInt64 AS t0 WHERE t0.ts < (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) * 1000) AND t0.ts >= (UNIX_TIMESTAMP(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP - INTERVAL '1' DAY) + INTERVAL '1' DAY) * 1000)",
        )

    def test_truncate_week_of_native_timestamp_column(self):
        start_time = ibis.now().truncate("week")
        end_time = ibis.now()
        expr = TableTimestamp.filter((_.ts >= start_time) & (_.ts < end_time)).select(_.name)

        sql = to_sql(expr)

        self.assertEqual(
            sql,
            "SELECT t0.name FROM TableTimestamp AS t0 WHERE t0.ts < CURRENT_TIMESTAMP AND t0.ts >= (DATE_TRUNC('WEEK', CURRENT_TIMESTAMP - INTERVAL '1' DAY) + INTERVAL '1' DAY)",
        )
